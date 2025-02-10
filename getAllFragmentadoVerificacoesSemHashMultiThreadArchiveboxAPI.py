import os
import sys
import logging
import json
import re
import shutil
import threading
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from bson.binary import Binary

# Importa a função da API do ArchiveBox a partir do módulo CLI para evitar circular imports
from archivebox.cli.archivebox_add import add as archivebox_add

# Verifica se o arquivo de URLs foi fornecido
if len(sys.argv) < 2:
    print("Erro: Caminho para o arquivo URL_LIST_FILE não fornecido.")
    print("Uso: python getAllFragmentadoVerificacoesSemHashMultiThreadArchiveboxAPI.py <URL_LIST_FILE>")
    sys.exit(1)

# Configurações e constantes
ARCHIVEBOX_DIR = Path("/Users/wellisonbertelli/Documents/Poder360_estagio/waybackmachine_maquina_do_tempo/archivebox/get")
URL_LIST_FILE = sys.argv[1]
DATABASE_NAME = "archivebox_db"
COLLECTION_NAME = "arquivos_da_home_obtidos_no_wayback_machine"
LOG_FILE = os.path.join(ARCHIVEBOX_DIR, "archive_and_upload.log")

# Configura o logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

# Regex para extração do timestamp
TIMESTAMP_REGEX = re.compile(r"/web/(\d{14})")
archive_lock = threading.Lock()

def conectarBanco():
    try:
        client = MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=5000)
        client.server_info()
        logging.info("Conexão ao MongoDB bem-sucedida!")
        return client
    except ServerSelectionTimeoutError as e:
        logging.error(f"Erro ao conectar ao MongoDB: {e}")
        return None

# Conectar ao MongoDB (o client é thread-safe)
client = conectarBanco()

class ArquivosDaHomeWaybackMachineModel:
    """Modelo de documento para o MongoDB."""
    def __init__(self, device, content, timestamp, isAdvertisingModified, advertising_id_when_isModified):
        self.device = device
        self.content = content
        self.timestamp = timestamp
        self.isAdvertisingModified = isAdvertisingModified
        self.advertising_id_when_isModified = advertising_id_when_isModified
    
    def to_dict(self):
        return {
            "device": self.device,
            "content": Binary(self.content.encode('utf-8')),
            "timestamp": self.timestamp,
            "isAdvertisingModified": self.isAdvertisingModified,
            "advertising_id_when_isModified": self.advertising_id_when_isModified
        }

def extract_wayback_timestamp_substring(url: str) -> str:
    """
    Extrai o timestamp (YYYYMMDDhhmmss) do Wayback Machine da URL.
    Retorna a string do timestamp ou None em caso de erro.
    """
    try:
        match = TIMESTAMP_REGEX.search(url)
        if match:
            return match.group(1)
        else:
            logging.error(f"Timestamp não encontrado na URL: {url}")
            return None
    except Exception as e:
        logging.error(f"Erro ao extrair timestamp da URL {url}: {e}")
        return None

def archive_url(url):
    """
    Arquiva uma URL usando a API Python do ArchiveBox (por meio da função 'add' do módulo CLI),
    processa o snapshot (lê o arquivo HTML), insere o documento no MongoDB, remove o snapshot
    e registra o sucesso ou erro.
    """
    success_log = Path(ARCHIVEBOX_DIR) / "success_insertInto_mongo.txt"
    error_log = Path(ARCHIVEBOX_DIR) / "error_insertInto_mongo.txt"

    try:
        # Chamar a API do ArchiveBox de forma sincronizada (para evitar problemas internos com SQLite)
        with archive_lock:
            # A função 'add' espera uma lista de URLs; definimos o diretório de saída
            links = archivebox_add(urls=[url], out_dir=ARCHIVEBOX_DIR)
        
        logging.info(f"ArchiveBox API executada com sucesso para a URL: {url}")

        if not links:
            error_message = f"A API do ArchiveBox não retornou resultados para a URL: {url}\n"
            logging.error(error_message)
            with open(error_log, 'a', encoding='utf-8') as ef:
                ef.write(error_message)
            return

        # Consideramos que há apenas um link, já que passamos uma única URL
        link = links[0]
        # Supondo que o objeto retornado possua o atributo 'archive_dir'
        snapshot_dir = Path(link.archive_dir)
        singlefile_html = snapshot_dir / "singlefile.html"

        if singlefile_html.exists():
            with open(singlefile_html, "r", encoding="utf-8") as f:
                logging.info(f"Snapshot encontrado: {singlefile_html}")
                timestamp_str = extract_wayback_timestamp_substring(url)
                if timestamp_str is not None:
                    dt_naive = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
                    dt_utc = dt_naive.replace(tzinfo=timezone.utc)
                    html_content = f.read()

                    if not html_content:
                        error_message = f"Conteúdo HTML vazio para URL: {url}\n"
                        logging.error(error_message)
                        with open(error_log, 'a', encoding='utf-8') as ef:
                            ef.write(error_message)
                    else:
                        page_model = ArquivosDaHomeWaybackMachineModel(
                            device='--window-size=1280,720',
                            content=html_content,
                            timestamp=dt_utc,
                            isAdvertisingModified=False,
                            advertising_id_when_isModified=None
                        )
                        page_dict = page_model.to_dict()

                        logging.debug(f"Inserindo documento no MongoDB para a URL: {url}")
                        if not client:
                            error_message = f"Falha na conexão com o MongoDB para URL: {url}\n"
                            logging.error(error_message)
                            with open(error_log, 'a', encoding='utf-8') as ef:
                                ef.write(error_message)
                            return

                        database = client[DATABASE_NAME]
                        collection = database[COLLECTION_NAME]
                        response = collection.insert_one(page_dict)
                        logging.info(f"Documento inserido com ID: {response.inserted_id} para URL: {url}")

                        try:
                            shutil.rmtree(snapshot_dir)
                            logging.info(f"Diretório {snapshot_dir} removido com sucesso.")
                        except Exception as rmtree_error:
                            error_message = f"Erro ao remover {snapshot_dir} para URL: {url} - {rmtree_error}\n"
                            logging.error(error_message)
                            with open(error_log, 'a', encoding='utf-8') as ef:
                                ef.write(error_message)

                        with open(success_log, 'a', encoding='utf-8') as sf:
                            sf.write(f"{url}\n")
                        logging.info(f"URL {url} registrada com sucesso.")
                else:
                    error_message = f"Erro na extração do timestamp para URL: {url}\n"
                    logging.error(error_message)
                    with open(error_log, 'a', encoding='utf-8') as ef:
                        ef.write(error_message)
        else:
            error_message = f"Arquivo singlefile.html não encontrado no snapshot para URL: {url}\n"
            logging.warning(error_message)
            with open(error_log, 'a', encoding='utf-8') as ef:
                ef.write(error_message)
    except Exception as ex:
        error_message = f"Ocorreu um erro inesperado para URL: {url} - {ex}\n"
        logging.error(error_message)
        with open(error_log, 'a', encoding='utf-8') as ef:
            ef.write(error_message)

# =============================
# Função Principal
# =============================
def main():
    logging.info("Iniciando o processo de arquivamento...")
    urls_file_path = Path(URL_LIST_FILE)
    if not urls_file_path.exists():
        logging.error(f"Arquivo {URL_LIST_FILE} não encontrado em {ARCHIVEBOX_DIR}.")
        sys.exit(1)

    with open(urls_file_path, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]

    if not urls:
        logging.warning("Nenhuma URL encontrada para arquivar.")
        sys.exit(0)

    success_log = Path(ARCHIVEBOX_DIR) / "success_insertInto_mongo.txt"
    if success_log.exists():
        with open(success_log, "r", encoding="utf-8") as sf:
            processed_urls = set(line.strip() for line in sf if line.strip())
        logging.info(f"{len(processed_urls)} URLs já foram processadas.")
    else:
        processed_urls = set()
        logging.info("Nenhuma URL processada anteriormente.")

    urls_to_process = [url for url in urls if url not in processed_urls]

    if not urls_to_process:
        logging.info("Todas as URLs já foram processadas com sucesso.")
        sys.exit(0)

    logging.info(f"{len(urls_to_process)} URLs serão processadas.")

    max_workers = 5  # Ajuste conforme necessário
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(archive_url, url): url for url in urls_to_process}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                future.result()
            except Exception as exc:
                logging.error(f"Erro na execução paralela para a URL {url}: {exc}")

if __name__ == "__main__":
    main()
