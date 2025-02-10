import asyncio
import hashlib
import logging
import os
import re
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from subprocess import CalledProcessError, run
from bson.binary import Binary

# Configurações gerais
ARCHIVEBOX_DIR = "/Users/wellisonbertelli/Documents/Poder360_estagio/waybackmachine_maquina_do_tempo/archivebox/get"
URL_LIST_FILE = sys.argv[1] if len(sys.argv) > 1 else None
DATABASE_NAME = "archivebox_db"
COLLECTION_NAME = "arquivos_da_home_obtidos_no_wayback_machine"
LOG_FILE = os.path.join(ARCHIVEBOX_DIR, "archive_and_upload.log")

# Configuração de logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s]: %(message)s',
)

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
            "advertising_id_when_isModified": self.advertising_id_when_isModified,
        }

async def archive_url(url):
    """Processa uma URL, arquiva e salva no MongoDB."""
    try:
        logging.info(f"Iniciando processamento para URL: {url}")
        result = run(
            ["archivebox", "add", url],
            cwd=ARCHIVEBOX_DIR,
            capture_output=True,
            text=True,
            check=True,
        )

        output = result.stdout
        logging.debug(f"Saída do ArchiveBox: {output}")

        match = re.search(r"> \./archive/([\w.]+)/?", output)
        if not match:
            logging.error(f"Snapshot não encontrado para URL: {url}")
            return

        base_path = match.group(1)
        snapshot_dir = Path(ARCHIVEBOX_DIR) / "archive" / base_path
        singlefile_html = snapshot_dir / "singlefile.html"

        if not singlefile_html.exists():
            logging.error(f"Arquivo singlefile.html não encontrado para URL: {url}")
            return

        with open(singlefile_html, "r", encoding="utf-8") as f:
            content = f.read()

        timestamp = extract_wayback_timestamp_substring(url)
        if not timestamp:
            logging.error(f"Timestamp inválido para URL: {url}")
            return

        dt_naive = datetime.strptime(timestamp, "%Y%m%d%H%M%S")
        dt_utc = dt_naive.replace(tzinfo=timezone.utc)

        document = ArquivosDaHomeWaybackMachineModel(
            device="--window-size=1280,720",
            content=content,
            timestamp=dt_utc,
            isAdvertisingModified=False,
            advertising_id_when_isModified=None,
        ).to_dict()

        async with MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=5000) as client:
            db = client[DATABASE_NAME]
            collection = db[COLLECTION_NAME]
            result = await collection.insert_one(document)
            logging.info(f"Documento inserido com ID: {result.inserted_id}")

    except CalledProcessError as e:
        logging.error(f"Erro ao executar ArchiveBox: {e.stderr}")
    except Exception as e:
        logging.error(f"Erro inesperado ao processar URL {url}: {e}")

def extract_wayback_timestamp_substring(url):
    """Extrai o timestamp do Wayback Machine (YYYYMMDDhhmmss)."""
    marker = "/web/"
    try:
        start_index = url.index(marker) + len(marker)
        timestamp = url[start_index:start_index + 14]
        return timestamp if len(timestamp) == 14 and timestamp.isdigit() else None
    except ValueError:
        logging.error(f"Marker '/web/' não encontrado na URL: {url}")
        return None

async def main():
    """Função principal para processar URLs."""
    if not URL_LIST_FILE:
        logging.error("Arquivo URL_LIST_FILE não fornecido.")
        sys.exit(1)

    urls_file_path = Path(URL_LIST_FILE)
    if not urls_file_path.exists():
        logging.error(f"Arquivo {URL_LIST_FILE} não encontrado.")
        sys.exit(1)

    with open(urls_file_path, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    success_log = Path(ARCHIVEBOX_DIR) / "success_insertInto_mongo.txt"
    processed_urls = set()

    if success_log.exists():
        with open(success_log, "r", encoding="utf-8") as sf:
            processed_urls.update(line.strip() for line in sf if line.strip())

    urls_to_process = [url for url in urls if url not in processed_urls]

    tasks = [archive_url(url) for url in urls_to_process]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
