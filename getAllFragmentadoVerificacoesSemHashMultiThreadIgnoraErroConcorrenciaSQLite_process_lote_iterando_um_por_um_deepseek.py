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
import sqlite3
import time

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

# Aumente aqui, já que seu iMac tem 8 cores / 16 threads
MAX_WORKERS = 16

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

def enable_wal_mode(db_path):
    """
    Habilita o modo WAL no banco de dados SQLite.
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('PRAGMA journal_mode=WAL;')
        result = cursor.fetchone()
        logging.info(f"Modo WAL habilitado no banco de dados: {result[0]}")
        conn.close()
    except Exception as e:
        logging.error(f"Erro ao habilitar o modo WAL: {e}")

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

def archive_url_batch(urls, retries=3, delay=5):
    """
    Arquiva uma lista de URLs usando o ArchiveBox, insere os snapshots no MongoDB,
    remove os snapshots e registra o sucesso ou erro.
    Tenta novamente em caso de erro de 'database locked' por até 'retries' vezes.
    """
    success_log = Path(ARCHIVEBOX_DIR) / "success_insertInto_mongo.txt"
    error_log = Path(ARCHIVEBOX_DIR) / "error_insertInto_mongo.txt"

    attempt = 0
    while attempt < retries:
        try:
            # Executa o ArchiveBox para cada URL no lote
            for url in urls:
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

            # Se chegou até aqui, deu tudo certo; sair do loop
            break

        except subprocess.CalledProcessError as e:
            if "database is locked" in e.stderr.lower():
                attempt += 1
                if attempt < retries:
                    logging.warning(
                        f"database locked para URL {url}, esperando {delay}s e tentando novamente "
                        f"({attempt}/{retries})"
                    )
                    time.sleep(delay)
                else:
                    logging.error(
                        f"database locked para URL {url} após {retries} tentativas. Abortando."
                    )
                    with open(error_log, 'a', encoding='utf-8') as ef:
                        ef.write(f"Erro de lock após {retries} tentativas para URL: {url}\n")
            else:
                # Se for outro tipo de erro, registrar e sair
                error_message = f"Erro ao arquivar {url}: {e.stderr}\n"
                logging.error(error_message)
                with open(error_log, 'a', encoding='utf-8') as ef:
                    ef.write(error_message)
                break

        except Exception as ex:
            if "database is locked" in str(ex).lower():
                attempt += 1
                if attempt < retries:
                    logging.warning(
                        f"database locked para URL {url}, esperando {delay}s e tentando novamente "
                        f"({attempt}/{retries})"
                    )
                    time.sleep(delay)
                else:
                    logging.error(
                        f"database locked para URL {url} após {retries} tentativas. Abortando."
                    )
                    with open(error_log, 'a', encoding='utf-8') as ef:
                        ef.write(f"Erro de lock após {retries} tentativas para URL: {url}\n")
            else:
                error_message = f"Ocorreu um erro inesperado para URL: {url} - {ex}\n"
                logging.error(error_message)
                with open(error_log, 'a', encoding='utf-8') as ef:
                    ef.write(error_message)
                break

# =============================
# Função Principal
# =============================

# Conectar ao MongoDB (o client é thread-safe)

client = conectarBanco()

def main():
    logging.info("Iniciando o processo de arquivamento...")

    # Habilitar o modo WAL no banco de dados SQLite do ArchiveBox
    enable_wal_mode(ARCHIVEBOX_INDEX_DB)

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

    # Agrupar as URLs em lotes de 10
    batch_size = 10
    url_batches = [urls_to_process[i:i + batch_size] for i in range(0, len(urls_to_process), batch_size)]

    # Processar os lotes de URLs em paralelo
    logging.info(f"Processando em paralelo com max_workers={MAX_WORKERS}")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_batch = {executor.submit(archive_url_batch, batch): batch for batch in url_batches}
        for future in as_completed(future_to_batch):
            batch = future_to_batch[future]
            try:
                future.result()
            except Exception as exc:
                # Aqui, se ocorrer erro e não for de concorrência, ele já foi logado
                logging.error(f"Erro na execução paralela para o lote de URLs: {batch}: {exc}")

if __name__ == "__main__":
    main()
