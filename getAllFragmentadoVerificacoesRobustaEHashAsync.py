import subprocess
import os
import sys
import sqlite3
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from datetime import datetime, timezone
import logging
import asyncio
from playwright.async_api import async_playwright
import json
from pathlib import Path
import re
from bson.binary import Binary
import shutil
import connect_local
import hashlib
import queue
import threading

# Configurações e verificações iniciais permanecem iguais
if len(sys.argv) < 2:
    print("Erro: Caminho para o arquivo URL_LIST_FILE não fornecido.")
    print("Uso: python getAllFragmentado.py <URL_LIST_FILE>")
    sys.exit(1)

ARCHIVEBOX_DIR = "/Users/wellisonbertelli/Documents/Poder360_estagio/waybackmachine_maquina_do_tempo/archivebox/get"
URL_LIST_FILE = sys.argv[1]

DATABASE_NAME = "archivebox_db"
COLLECTION_NAME = "arquivos_da_home_obtidos_no_wayback_machine"

ARCHIVEBOX_INDEX_DB = os.path.join(ARCHIVEBOX_DIR, "index.sqlite3")
LOG_FILE = os.path.join(ARCHIVEBOX_DIR, "archive_and_upload.log")

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

# Classes e funções auxiliares permanecem iguais
class ArquivosDaHomeWaybackMachineModel:
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
    marker = "/web/"
    try:
        start_index = url.index(marker) + len(marker)
        timestamp_str = url[start_index : start_index + 14]
        if len(timestamp_str) == 14 and timestamp_str.isdigit() and len(url) == 67:
            return timestamp_str
        return None
    except ValueError:
        return None

def conectarBanco():
    try:
        client = MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=5000)
        client.server_info()
        return client
    except ServerSelectionTimeoutError:
        return None

def calcular_hash(caminho_arquivo, algoritmo='md5'):
    try:
        hash_func = getattr(hashlib, algoritmo)()
        with open(caminho_arquivo, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_func.update(chunk)
        return hash_func.hexdigest()
    except Exception:
        return None

def verificar_integridade(caminho_original, caminho_recuperado, algoritmo='md5'):
    hash_original = calcular_hash(caminho_original, algoritmo)
    hash_recuperado = calcular_hash(caminho_recuperado, algoritmo)
    if hash_original and hash_recuperado and hash_original == hash_recuperado:
        logging.info("Integridade verificada: os arquivos são idênticos.")
    else:
        logging.warning("Atenção: os arquivos diferem.")

# Variáveis globais para controle de threads
file_write_lock = threading.Lock()
success_log = Path(ARCHIVEBOX_DIR) / "success_insertInto_mongo.txt"
error_log = Path(ARCHIVEBOX_DIR) / "error_insertInto_mongo.txt"

def log_success(url):
    with file_write_lock:
        with open(success_log, 'a', encoding='utf-8') as sf:
            sf.write(f"{url}\n")

def log_error(url, error_message):
    with file_write_lock:
        with open(error_log, 'a', encoding='utf-8') as ef:
            ef.write(f"{url}: {error_message}\n")

def process_snapshot(url, snapshot_dir):
    try:
        snapshot_dir = Path(snapshot_dir)
        singlefile_html = snapshot_dir / "singlefile.html"
        
        if not singlefile_html.exists():
            log_error(url, "Arquivo singlefile.html não encontrado")
            return

        with open(singlefile_html, "r", encoding="utf-8") as f:
            html_content = f.read()
            if not html_content:
                log_error(url, "Conteúdo HTML vazio")
                return

            timestamp_str = extract_wayback_timestamp_substring(url)
            if not timestamp_str:
                log_error(url, "Timestamp inválido")
                return

            dt_naive = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
            dt_utc = dt_naive.replace(tzinfo=timezone.utc)

            page_model = ArquivosDaHomeWaybackMachineModel(
                device='--window-size=1280,720',
                content=html_content,
                timestamp=dt_utc,
                isAdvertisingModified=False,
                advertising_id_when_isModified=None
            )
            page_dict = page_model.to_dict()

            client = conectarBanco()
            if not client:
                log_error(url, "Falha na conexão com MongoDB")
                return

            database = client[DATABASE_NAME]
            collection = database[COLLECTION_NAME]
            response = collection.insert_one(page_dict)
            
            if response.inserted_id:
                caminho_recuperado = snapshot_dir / f"recuperado_{timestamp_str}.html"
                with open(caminho_recuperado, 'w', encoding='utf-8') as f_rec:
                    f_rec.write(html_content)
                
                verificar_integridade(str(singlefile_html), str(caminho_recuperado))
                
                try:
                    shutil.rmtree(snapshot_dir)
                    logging.info(f"Diretório {snapshot_dir} excluído")
                except Exception as e:
                    log_error(url, f"Erro ao excluir diretório: {str(e)}")
                
                log_success(url)
            else:
                log_error(url, "Falha na inserção no MongoDB")

    except Exception as e:
        log_error(url, f"Erro inesperado: {str(e)}")
    finally:
        if 'client' in locals():
            client.close()

def process_snapshot_worker(snapshot_queue):
    while True:
        item = snapshot_queue.get()
        if item is None:
            break
        url, snapshot_dir = item
        try:
            process_snapshot(url, snapshot_dir)
        finally:
            snapshot_queue.task_done()

def archive_url(url):
    try:
        result = subprocess.run(
            ["archivebox", "add", url],
            cwd=ARCHIVEBOX_DIR,
            capture_output=True,
            text=True,
            check=True
        )

        output = result.stdout
        archive_path_match = re.search(r"> \./archive/([\w.]+)/?", output)
        if archive_path_match:
            base_path = archive_path_match.group(1)
            return Path(ARCHIVEBOX_DIR) / "archive" / base_path
        return None

    except subprocess.CalledProcessError as e:
        log_error(url, f"Erro no ArchiveBox: {e.stderr}")
    except Exception as e:
        log_error(url, f"Erro inesperado: {str(e)}")
    return None

def main():
    logging.info("Iniciando processo de arquivamento")
    urls_file_path = Path(URL_LIST_FILE)
    
    if not urls_file_path.exists():
        logging.error("Arquivo de URLs não encontrado")
        sys.exit(1)

    with open(urls_file_path, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    processed_urls = set()
    if success_log.exists():
        with open(success_log, "r", encoding="utf-8") as sf:
            processed_urls = set(line.strip() for line in sf if line.strip())

    urls_to_process = [url for url in urls if url not in processed_urls]

    if not urls_to_process:
        logging.info("Todas URLs já processadas")
        sys.exit(0)

    # Configurar fila e workers
    snapshot_queue = queue.Queue()
    num_workers = 4
    workers = []
    for _ in range(num_workers):
        worker = threading.Thread(target=process_snapshot_worker, args=(snapshot_queue,))
        worker.daemon = True
        worker.start()
        workers.append(worker)

    # Processar URLs sequencialmente e enfileirar snapshots
    for url in urls_to_process:
        logging.info(f"Arquivando URL: {url}")
        snapshot_dir = archive_url(url)
        if snapshot_dir:
            snapshot_queue.put((url, snapshot_dir))

    # Esperar processamento concluir
    snapshot_queue.join()

    # Encerrar workers
    for _ in range(num_workers):
        snapshot_queue.put(None)
    for worker in workers:
        worker.join()

    logging.info("Processamento concluído")

if __name__ == "__main__":
    main()