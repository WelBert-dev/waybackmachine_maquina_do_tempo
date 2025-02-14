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

# Verificar se o diretório de log existe, caso contrário, criá-lo
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

# =============================
# Função para arquivar LOTE de URLs
# =============================
def archive_urls_chunk(urls_chunk, retries=3, delay=5):
    """
    Arquiva um lote de URLs (urls_chunk) usando o ArchiveBox de uma só vez.
    Em caso de 'database locked', tenta novamente até 'retries' vezes, com 'delay' segundos entre tentativas.
    """
    success_log = Path(ARCHIVEBOX_DIR) / "success_insertInto_mongo.txt"
    error_log = Path(ARCHIVEBOX_DIR) / "error_insertInto_mongo.txt"

    # Precisamos mapear URL -> snapshot_dir depois do subprocess, então
    # vamos manter um dicionário temporário. Mas a saída do ArchiveBox
    # não diz explicitamente qual URL gerou qual snapshot,
    # apenas na mesma ordem que são processadas.
    #
    # DICA: Se você usa ArchiveBox >= v0.11, pode usar `--json` no add,
    # mas aqui vamos parsear a saída texto, assumindo que a ordem das URLs
    # corresponde à ordem dos snapshots criados.

    attempt = 0
    while attempt < retries:
        try:
            # Executa o ArchiveBox com todas as URLs do chunk
            cmd = ["archivebox", "add"] + urls_chunk
            result = subprocess.run(
                cmd,
                cwd=ARCHIVEBOX_DIR,
                capture_output=True,
                text=True,
                check=True
            )

            logging.info(f"ArchiveBox executado com sucesso para o lote de {len(urls_chunk)} URLs.")
            output = result.stdout
            logging.debug(f"Saída do ArchiveBox para o lote:\n{output}")

            # Extrair todos os caminhos de snapshot na ordem em que aparecerem
            # Note que cada URL resultará em 1 match (se deu certo).
            # A suposição aqui é que a ordem dos matches coincide com a ordem das URLs passadas,
            # pois, normalmente, o ArchiveBox processa nessa mesma sequência.
            snapshot_dirs = ARCHIVE_PATH_REGEX.findall(output)  # lista de strings (hashes)
            
            if len(snapshot_dirs) != len(urls_chunk):
                # Se o número de snapshots encontrados não bater, algo deu errado
                error_message = (
                    f"Número de snapshots ({len(snapshot_dirs)}) diferente do número de URLs "
                    f"({len(urls_chunk)}) para o chunk: {urls_chunk}\n"
                )
                logging.error(error_message)
                with open(error_log, 'a', encoding='utf-8') as ef:
                    ef.write(error_message)
                # Ainda assim, seguimos para tentar processar o que der
                # (ou você pode dar `return` para interromper).
            
            # Processar cada URL juntamente com seu snapshot (quando houver).
            for idx, url in enumerate(urls_chunk):
                try:
                    # Se não houver snapshot_dir correspondente, pula
                    if idx >= len(snapshot_dirs):
                        error_message = f"Snapshot não encontrado para a URL: {url}\n"
                        logging.error(error_message)
                        with open(error_log, 'a', encoding='utf-8') as ef:
                            ef.write(error_message)
                        continue

                    base_path = snapshot_dirs[idx]
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
                                    # Monta o documento e insere no Mongo
                                    page_model = ArquivosDaHomeWaybackMachineModel(
                                        device='--window-size=1280,720',
                                        content=html_content,
                                        timestamp=dt_utc,
                                        isAdvertisingModified=False,
                                        advertising_id_when_isModified=None
                                    )
                                    page_dict = page_model.to_dict()

                                    if not client:
                                        error_message = f"Falha na conexão com o MongoDB para URL: {url}\n"
                                        logging.error(error_message)
                                        with open(error_log, 'a', encoding='utf-8') as ef:
                                            ef.write(error_message)
                                        continue  # não adianta prosseguir

                                    database = client[DATABASE_NAME]
                                    collection = database[COLLECTION_NAME]
                                    response = collection.insert_one(page_dict)
                                    logging.info(f"Documento inserido com ID: {response.inserted_id} para URL: {url}")

                                    # Remover o diretório do snapshot
                                    try:
                                        shutil.rmtree(snapshot_dir)
                                        logging.info(f"Diretório {snapshot_dir} removido com sucesso.")
                                    except Exception as rmtree_error:
                                        error_message = (
                                            f"Erro ao remover {snapshot_dir} para URL: {url} - {rmtree_error}\n"
                                        )
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
                        error_message = f"singlefile.html não encontrado para URL: {url}\n"
                        logging.error(error_message)
                        with open(error_log, 'a', encoding='utf-8') as ef:
                            ef.write(error_message)

                except Exception as e_individual:
                    error_message = f"Erro processando URL {url} no chunk: {e_individual}\n"
                    logging.error(error_message)
                    with open(error_log, 'a', encoding='utf-8') as ef:
                        ef.write(error_message)

            # Se chegou até aqui, concluímos o lote com sucesso (ou com avisos).
            break

        except subprocess.CalledProcessError as e:
            # Verifica se o erro menciona "database is locked"
            if "database is locked" in e.stderr.lower():
                attempt += 1
                if attempt < retries:
                    logging.warning(
                        f"database locked para o chunk, esperando {delay}s e tentando novamente "
                        f"({attempt}/{retries})"
                    )
                    time.sleep(delay)
                else:
                    logging.error(
                        f"database locked para o chunk após {retries} tentativas. Abortando."
                    )
                    with open(error_log, 'a', encoding='utf-8') as ef:
                        ef.write(f"Erro de lock após {retries} tentativas para chunk: {urls_chunk}\n")
            else:
                # Se for outro tipo de erro, registrar e sair
                error_message = f"Erro ao arquivar chunk {urls_chunk}: {e.stderr}\n"
                logging.error(error_message)
                with open(error_log, 'a', encoding='utf-8') as ef:
                    ef.write(error_message)
                break

        except Exception as ex:
            if "database is locked" in str(ex).lower():
                attempt += 1
                if attempt < retries:
                    logging.warning(
                        f"database locked para o chunk, esperando {delay}s e tentando novamente "
                        f"({attempt}/{retries})"
                    )
                    time.sleep(delay)
                else:
                    logging.error(
                        f"database locked para o chunk após {retries} tentativas. Abortando."
                    )
                    with open(error_log, 'a', encoding='utf-8') as ef:
                        ef.write(f"Erro de lock após {retries} tentativas para chunk: {urls_chunk}\n")
            else:
                error_message = f"Ocorreu um erro inesperado para chunk: {urls_chunk} - {ex}\n"
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

    logging.info(f"{len(urls_to_process)} URLs serão processadas em grupos de 10.")

    # Quebrar as URLs em chunks de 10
    chunk_size = 10
    for i in range(0, len(urls_to_process), chunk_size):
        urls_chunk = urls_to_process[i:i+chunk_size]
        archive_urls_chunk(urls_chunk)

if __name__ == "__main__":
    main()
