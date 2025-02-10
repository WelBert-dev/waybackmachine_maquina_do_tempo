import subprocess
import os
import sys
import sqlite3
from pymongo import MongoClient
from datetime import datetime, timezone
import logging
import asyncio
from playwright.async_api import async_playwright
import json
from pathlib import Path
import re
from bson.binary import Binary
import shutil  # Importar shutil
import connect_local

# Verificar se o argumento foi fornecido
if len(sys.argv) < 2:
    print("Erro: Caminho para o arquivo URL_LIST_FILE não fornecido.")
    print("Uso: python getAllFragmentado.py <URL_LIST_FILE>")
    sys.exit(1)

# Configurações
ARCHIVEBOX_DIR = "/Users/wellisonbertelli/Documents/Poder360_estagio/waybackmachine_maquina_do_tempo/archivebox/get"  # Substitua pelo caminho correto
URL_LIST_FILE = sys.argv[1]  # Recebe o caminho do URL_LIST_FILE como argumento

DATABASE_NAME = "archivebox_db"
COLLECTION_NAME = "arquivos_da_home_obtidos_no_wayback_machine"

ARCHIVEBOX_INDEX_DB = os.path.join(ARCHIVEBOX_DIR, "index.sqlite3")
LOG_FILE = os.path.join(ARCHIVEBOX_DIR, "archive_and_upload.log")

# Configurar o logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

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
    """
    Extrai o timestamp do Wayback Machine (YYYYMMDDhhmmss) de uma URL,
    utilizando substring (slicing). Retorna apenas a string do timestamp.
    """

    marker = "/web/" # 28 caracteres
    try:
        # Encontra o índice inicial do trecho "/web/"
        start_index = 28 # url.index(marker) + len(marker)  # Posição logo após '/web/'
    except ValueError:
        # Se não encontrar "/web/", retorna None (ou pode lançar uma exceção)
        return None

    # Queremos os 14 caracteres após start_index
    timestamp_str = url[start_index : start_index + 14]

    # Opcionalmente, podemos verificar se realmente são 14 dígitos
    # (se for importante garantir que seja só dígito e tenha 14 chars)

    print(f'Len da string url :>> {len(url)}')
    if len(timestamp_str) == 14 and timestamp_str.isdigit() and len(url) == 67:
        return timestamp_str
    else:
        return None

def archive_url(url):
    """
    Arquiva uma URL usando o ArchiveBox, salva no MongoDB, realiza operações de recuperação,
    verifica a integridade dos dados e registra o sucesso ou erro em arquivos de texto.
    """
    # Definir os caminhos para os arquivos de log
    success_log = Path(ARCHIVEBOX_DIR) / "success_insertInto_mongo.txt"
    error_log = Path(ARCHIVEBOX_DIR) / "error_insertInto_mongo.txt"

    try:
        result = subprocess.run(
            [
                "archivebox",
                "add",
                url
            ],
            cwd=ARCHIVEBOX_DIR,
            capture_output=True,
            text=True,
            check=True
        )

        print('Archivebox executado...')

        # Verificar se a saída padrão contém informações esperadas
        output = result.stdout
        print(output)  # Depuração: exibe a saída completa do comando

        # Usar regex para encontrar o caminho do snapshot
        archive_path_match = re.search(r"> \./archive/([\w.]+)/?", output)
        if archive_path_match:
            base_path = archive_path_match.group(1)
            snapshot_dir = Path(ARCHIVEBOX_DIR) / "archive" / base_path

            # Caminho para arquivos gerados
            singlefile_html = snapshot_dir / "singlefile.html"
            # dom_html = snapshot_dir / "dom.html"

            # Verificar se os arquivos existem
            if singlefile_html.exists():
                with open(singlefile_html, "r", encoding="utf-8") as f:

                    print(f'Singlefile baixado :>> {singlefile_html}')

                    timestamp_str = extract_wayback_timestamp_substring(url)

                    if (not timestamp_str is None):

                        # Converte a string para um objeto datetime "naive" (sem timezone)
                        dt_naive = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")

                        # Atribui o timezone UTC
                        dt_utc = dt_naive.replace(tzinfo=timezone.utc)

                        print("Objeto datetime em UTC :>>", dt_utc)

                        print(f'Timestampstring :>> {timestamp_str}')

                        html_content = f.read()

                        page_model = ArquivosDaHomeWaybackMachineModel('--window-size=1280,720', html_content, dt_utc, False, None)
                        page_dict = page_model.to_dict()

                        client = connect_local.conectarBanco()

                        database = client[DATABASE_NAME]
                        collection = database[COLLECTION_NAME]

                        response = collection.insert_one(page_dict)

                        print(f'Response do banco: {response}')

                        # Verificar se a inserção foi bem-sucedida
                        if response.inserted_id:
                            print("Documento inserido com sucesso no MongoDB.")

                            # **Excluir o diretório snapshot_dir recursivamente**
                            try:
                                shutil.rmtree(snapshot_dir)
                                print(f"Diretório {snapshot_dir} excluído com sucesso.")
                            except Exception as rmtree_error:
                                print(f"Erro ao excluir o diretório {snapshot_dir}: {rmtree_error}")

                            # **Registrar o sucesso**
                            with open(success_log, 'a', encoding='utf-8') as sf:
                                sf.write(f"{url}\n")
                            print(f"URL registrada em {success_log}.")

                        else:
                            print("Falha na inserção do documento no MongoDB.")

                             # Registrar o erro
                            with open(error_log, 'a', encoding='utf-8') as ef:
                                ef.write(f"{url}\n")
                    else:
                        logging.error(f"Erro na extração do timestamp na URL :>> {url}")

                        # Registrar o erro
                        with open(error_log, 'a', encoding='utf-8') as ef:
                            ef.write(f"{url}\n")
            else:
                logging.warning(f"Arquivo singlefile_html não encontrado em {snapshot_dir}")

        else:
            logging.warning("Não foi possível encontrar o caminho do snapshot na saída.")

    except subprocess.CalledProcessError as e:
        logging.error(f"Erro ao arquivar {url}: {e.stderr}")
    except json.JSONDecodeError as e:
        logging.error(f"Não foi possível decodificar o JSON retornado pelo ArchiveBox: {e}")
    except KeyError as e:
        logging.error(f"Campo ausente no JSON do ArchiveBox: {e}")

def main():
    # Verificar se o arquivo de URLs existe
    print('Iniciando...')
    urls_file_path = os.path.join(URL_LIST_FILE)
    if not os.path.exists(urls_file_path):
        logging.error(f"Arquivo {URL_LIST_FILE} não encontrado em {ARCHIVEBOX_DIR}.")
        sys.exit(1)
    
    # Ler as URLs do arquivo
    with open(urls_file_path, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]

    if not urls:
        logging.warning("Nenhuma URL encontrada para arquivar.")
        sys.exit(0)
    
    # Arquivar cada URL
    for url in urls:
        archive_url(url)

if __name__ == "__main__":
    main()
