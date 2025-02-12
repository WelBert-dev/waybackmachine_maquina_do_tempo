import subprocess
import os
import sys
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from datetime import datetime, timezone
import logging
import json
from pathlib import Path
import re
from bson.binary import Binary
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

# Verificar se o argumento foi fornecido
if len(sys.argv) < 2:
    print("Erro: Caminho para o arquivo URL_LIST_FILE não fornecido.")
    print("Uso: python getAllFragmentado.py <URL_LIST_FILE>")
    sys.exit(1)

# =============================
# Configurações e Constantes
# =============================
ARCHIVEBOX_DIR = "/Users/wellisonbertelli/waybackmachine_maquina_do_tempo/archivebox/get"  # Substitua pelo caminho correto
URL_LIST_FILE = sys.argv[1]  # Recebe o caminho do URL_LIST_FILE como argumento

DATABASE_NAME = "archivebox_db"
COLLECTION_NAME = "arquivos_da_home_obtidos_no_wayback_machine"

ARCHIVEBOX_INDEX_DB = os.path.join(ARCHIVEBOX_DIR, "index.sqlite3")
LOG_FILE = os.path.join(ARCHIVEBOX_DIR, "archive_and_upload.log")

# Verificar se o diretório existe, caso contrário, criá-lo
log_dir = os.path.dirname(LOG_FILE)
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Configurar o logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

# Compilar a regex para extrair o caminho do snapshot (compilada uma única vez)
ARCHIVE_PATH_REGEX = re.compile(r"> \./archive/([\w.]+)/?")

# =============================
# Funções de apoio
# =============================
def conectarBanco():
    """Estabelece a conexão com o MongoDB."""
    try:
        client = MongoClient(
            "mongodb://127.0.0.1:27017",
            serverSelectionTimeoutMS=5000  # 5 segundos
        )
        client.server_info()
        logging.info("Conexão ao MongoDB bem-sucedida!")
        return client
    except ServerSelectionTimeoutError as e:
        logging.error(f"Erro ao conectar ao MongoDB: {e}")
        return None

# Conectar ao MongoDB (o client é thread-safe)
client = conectarBanco()

def retry_with_backoff(func, max_retries=5, initial_delay=0.1):
    retries = 0
    while retries < max_retries:
        try:
            return func()
        except Exception as e:
            if "database is locked" in str(e).lower():
                retries += 1
                delay = initial_delay * (2 ** retries) + random.uniform(0, 0.1)
                time.sleep(delay)
            else:
                raise e
    raise Exception(f"Max retries ({max_retries}) exceeded for function {func.__name__}")

def archive_url_with_retry(url):
    retry_with_backoff(lambda: archive_url(url))

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
    Extrai o timestamp do Wayback Machine (YYYYMMDDhhmmss) da URL.
    Retorna a string do timestamp ou None em caso de erro.
    """
    marker = "/web/"
    try:
        start_index = url.index(marker) + len(marker)
    except ValueError:
        logging.error(f"Marker '/web/' não encontrado na URL: {url}")
        return None

    timestamp_str = url[start_index: start_index + 14]
    if len(timestamp_str) == 14 and timestamp_str.isdigit():
        return timestamp_str
    else:
        logging.error(f"Timestamp inválido extraído da URL: {url}")
        return None

def archive_url(url):
    """
    Arquiva uma URL usando o ArchiveBox, insere o snapshot no MongoDB, remove o snapshot e
    registra o sucesso ou erro. Caso o erro esteja relacionado a concorrência (database locked),
    ele será ignorado.
    """
    success_log = Path(ARCHIVEBOX_DIR) / "success_insertInto_mongo.txt"
    error_log = Path(ARCHIVEBOX_DIR) / "error_insertInto_mongo.txt"

    try:
        # Executar o comando ArchiveBox sem lock (concorrência pode gerar erros no SQLite)
        result = subprocess.run(
            ["archivebox", "add", url],
            cwd=ARCHIVEBOX_DIR,
            capture_output=True,
            text=True,
            check=True
        )
        logging.info(f"ArchiveBox executado com sucesso para a URL: {url}")
        output = result.stdout
        logging.debug(f"Saída do ArchiveBox para a URL {url}: {output}")

        # Extrair o caminho do snapshot
        archive_path_match = ARCHIVE_PATH_REGEX.search(output)
        if archive_path_match:
            base_path = archive_path_match.group(1)
            snapshot_dir = Path(ARCHIVEBOX_DIR) / "archive" / base_path
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

                            # Remover o diretório do snapshot
                            try:
                                shutil.rmtree(snapshot_dir)
                                logging.info(f"Diretório {snapshot_dir} removido com sucesso.")
                            except Exception as rmtree_error:
                                error_message = f"Erro ao remover {snapshot_dir} para URL: {url} - {rmtree_error}\n"
                                logging.error(error_message)
                                with open(error_log, 'a', encoding='utf-8') as ef:
                                    ef.write(error_message)

                            # Registrar o sucesso
                            with open(success_log, 'a', encoding='utf-8') as sf:
                                sf.write(f"{url}\n")
                            logging.info(f"URL {url} registrada com sucesso.")
                    else:
                        error_message = f"Erro na extração do timestamp para URL: {url}\n"
                        logging.error(error_message)
                        with open(error_log, 'a', encoding='utf-8') as ef:
                            ef.write(error_message)
            else:
                error_message = f"Snapshot não encontrado na saída para URL: {url}\n"
                logging.warning(error_message)
                with open(error_log, 'a', encoding='utf-8') as ef:
                    ef.write(error_message)
        else:
            error_message = f"Regex não encontrou o caminho do snapshot para URL: {url}\n"
            logging.warning(error_message)
            with open(error_log, 'a', encoding='utf-8') as ef:
                ef.write(error_message)
    except subprocess.CalledProcessError as e:
        # Se o erro for de concorrência, ignora-o
        if "database is locked" in e.stderr.lower():
            logging.warning(f"Ignorando erro de concorrência (database locked) para URL {url}: {e.stderr}")
            return
        else:
            error_message = f"Erro ao arquivar {url}: {e.stderr}\n"
            logging.error(error_message)
            with open(error_log, 'a', encoding='utf-8') as ef:
                ef.write(error_message)
    except (json.JSONDecodeError, KeyError) as e:
        error_message = f"Erro ao processar JSON ou campo ausente para URL: {url} - {e}\n"
        logging.error(error_message)
        with open(error_log, 'a', encoding='utf-8') as ef:
            ef.write(error_message)
    except Exception as ex:
        # Se for erro de concorrência, ignora-o; caso contrário, registra o erro
        if "database is locked" in str(ex).lower():
            logging.warning(f"Ignorando erro de concorrência (database locked) para URL {url}: {ex}")
            return
        else:
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

    # Ler as URLs, ignorando linhas vazias e comentários
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

    # Processar as URLs em paralelo
    max_workers = 5  # Ajuste conforme necessário
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(archive_url_with_retry, url): url for url in urls_to_process}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                future.result()
            except Exception as exc:
                logging.error(f"Erro na execução paralela para a URL {url}: {exc}")

if __name__ == "__main__":
    main()
