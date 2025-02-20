import os
import sys
import requests
import time
import logging
import subprocess
import sqlite3
from pathlib import Path
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from datetime import datetime, timezone
from bson.binary import Binary
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
import shutil

# =======================================
# CONFIGURAÇÕES
# =======================================
WAYBACK_CDX_API = "http://web.archive.org/cdx/search/cdx"
ARCHIVEBOX_DIR = "/Users/wellisonbertelli/Documents/Poder360_estagio/waybackmachine_maquina_do_tempo/archivebox/get"  # Substitua pelo caminho correto
CHUNK_SIZE = 1  # Quantas URLs por subprocess do ArchiveBox
RETRIES = 3      # Número de tentativas em caso de "database locked"
DELAY = 5        # Tempo (s) de espera entre tentativas
LOG_FILE = os.path.join(ARCHIVEBOX_DIR, "wayback_download_generica_outro_prompt.log")
DATABASE_NAME = "archivebox_db"
COLLECTION_NAME = "arquivos_da_home_obtidos_no_wayback_machine"

# Aumente aqui, já que seu iMac tem 8 cores / 16 threads, ate 16 vai, mas perde muito com lock do sql
MAX_WORKERS = 1

# Compilar a regex para extrair o caminho do snapshot (compilada uma única vez)
ARCHIVE_PATH_REGEX = re.compile(r"> \./archive/([\w.]+)/?")

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

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

def get_wayback_snapshots(url_or_domain: str) -> List[str]:
    """
    Retorna uma lista de snapshots (timestamp, original_url) da Wayback Machine
    para um determinado domínio ou URL, usando a API de CDX do Wayback.

    Exemplo de uso:
        snapshots = get_wayback_snapshots("example.com")
        # ou
        snapshots = get_wayback_snapshots("https://example.com/foo/bar")
    """
    logging.info(f"Consultando a API CDX para '{url_or_domain}'...")

    # Parâmetros básicos para obter TODAS as capturas
    params = {
        "url": url_or_domain,
        "output": "json",
        "collapse": "digest",   # remove capturas duplicadas
        "fl": "timestamp,original", 
        "filter": "statuscode:200",  # filtra apenas capturas com HTTP 200 (opcional)
        "from": "2015",        # ano inicial
        "to": "202212",          # ano final (ou algo mais atual)
    }

    try:
        r = requests.get(WAYBACK_CDX_API, params=params, timeout=30)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao consultar Wayback CDX API: {e}")
        return []

    data = r.json()  # O primeiro elemento costuma ser o cabeçalho se 'output=json'.
    if not data or len(data) <= 1:
        logging.info("Nenhuma captura encontrada ou dados vazios.")
        return []

    # A primeira linha é o cabeçalho (timestamp, original)
    # As demais são os registros
    snapshots = []
    for row in data[1:]:
        if len(row) < 2:
            continue
        timestamp, original_url = row[0], row[1]

        # Montar a URL completa do Wayback
        # Formato: https://web.archive.org/web/<TIMESTAMP>/<ORIGINAL_URL>
        wayback_url = f"http://web.archive.org/web/{timestamp}if_/{original_url}"
        snapshots.append(wayback_url)

    # Agora invertendo a lista para que o mais novo (último timestamp) apareça primeiro.
    snapshots.reverse()

    logging.info(f"Foram encontradas {len(snapshots)} capturas no CDX (ordem do mais novo p/ mais antigo).")
    return snapshots

def save_urls_to_file(urls: List[str], file_path: str) -> None:
    """Salva a lista de URLs em um arquivo de texto (um por linha)."""
    with open(file_path, "w", encoding="utf-8") as f:
        for u in urls:
            f.write(u + "\n")
    logging.info(f"URLs salvas em {file_path}.")

def load_urls_from_file(file_path: str) -> List[str]:
    """Carrega as URLs de um arquivo (ignora linhas vazias ou comentários)."""
    if not os.path.exists(file_path):
        return []
    with open(file_path, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    return lines

def enable_wal_mode(db_path):
    """Ativa modo WAL no SQLite."""
    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute("PRAGMA journal_mode = WAL;")
        res = c.fetchone()
        logging.info(f"WAL mode habilitado, journal_mode={res[0]}")
        conn.close()
    except Exception as e:
        logging.error(f"Erro ao habilitar WAL: {e}")

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

def archive_urls_chunk(urls_chunk: List[str]) -> None:
    """
    Chama `archivebox add` para um lote (chunk) de URLs.
    Faz RETRIES em caso de 'database locked'.
    """
    success_log_path = Path(ARCHIVEBOX_DIR) / "success_wayback_urls.txt"
    error_log_path = Path(ARCHIVEBOX_DIR) / "error_wayback_urls.txt"

    attempt = 0
    while attempt < RETRIES:
        try:
            cmd = ["archivebox", "add"] + urls_chunk
            result = subprocess.run(
                cmd,
                cwd=ARCHIVEBOX_DIR,
                capture_output=True,
                text=True,
                check=True
            )
            # Se chegou aqui, deu certo
            logging.info(f"ArchiveBox: chunk de {len(urls_chunk)} URLs adicionado com sucesso.")

            output = result.stdout
            logging.info(f"Saída do ArchiveBox para o lote:\n{output}")

            # Registrar sucesso individual
            with open(success_log_path, "a", encoding="utf-8") as sf:
                for u in urls_chunk:
                    sf.write(u + "\n")
            break

            # Extrair todos os caminhos de snapshot na ordem em que aparecerem
            # Note que cada URL resultará em 1 match (se deu certo).
            # A suposição aqui é que a ordem dos matches coincide com a ordem das URLs passadas,
            # pois, normalmente, o ArchiveBox processa nessa mesma sequência.
        #     snapshot_dirs = ARCHIVE_PATH_REGEX.findall(output)  # lista de strings (hashes)
            
        #     if len(snapshot_dirs) != len(urls_chunk):
        #         # Se o número de snapshots encontrados não bater, algo deu errado
        #         error_message = (
        #             f"Número de snapshots ({len(snapshot_dirs)}) diferente do número de URLs "
        #             f"({len(urls_chunk)}) para o chunk: {urls_chunk}\n"
        #         )
        #         logging.error(error_message)
        #         with open(error_log_path, 'a', encoding='utf-8') as ef:
        #             ef.write(error_message)
        #         # Ainda assim, seguimos para tentar processar o que der
        #         # (ou você pode dar `return` para interromper).
            
        #     # Processar cada URL juntamente com seu snapshot (quando houver).
        #     for idx, url in enumerate(urls_chunk):
        #         try:
        #             # Se não houver snapshot_dir correspondente, pula
        #             if idx >= len(snapshot_dirs):
        #                 error_message = f"Snapshot não encontrado para a URL: {url}\n"
        #                 logging.error(error_message)
        #                 with open(error_log_path, 'a', encoding='utf-8') as ef:
        #                     ef.write(error_message)
        #                 continue

        #             base_path = snapshot_dirs[idx]
        #             snapshot_dir = Path(ARCHIVEBOX_DIR) / "archive" / base_path
        #             singlefile_html = snapshot_dir / "singlefile.html"

        #             if singlefile_html.exists():
        #                 with open(singlefile_html, "r", encoding="utf-8") as f:
        #                     logging.info(f"Snapshot encontrado: {singlefile_html}")
        #                     timestamp_str = extract_wayback_timestamp_substring(url)
        #                     if timestamp_str is not None:
        #                         dt_naive = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
        #                         dt_utc = dt_naive.replace(tzinfo=timezone.utc)
        #                         html_content = f.read()

        #                         if not html_content:
        #                             error_message = f"Conteúdo HTML vazio para URL: {url}\n"
        #                             logging.error(error_message)
        #                             with open(error_log_path, 'a', encoding='utf-8') as ef:
        #                                 ef.write(error_message)
        #                         else:
        #                             # Monta o documento e insere no Mongo
        #                             page_model = ArquivosDaHomeWaybackMachineModel(
        #                                 device='--window-size=1280,720',
        #                                 content=html_content,
        #                                 timestamp=dt_utc,
        #                                 isAdvertisingModified=False,
        #                                 advertising_id_when_isModified=None
        #                             )
        #                             page_dict = page_model.to_dict()

        #                             if not client:
        #                                 error_message = f"Falha na conexão com o MongoDB para URL: {url}\n"
        #                                 logging.error(error_message)
        #                                 with open(error_log_path, 'a', encoding='utf-8') as ef:
        #                                     ef.write(error_message)
        #                                 continue  # não adianta prosseguir

        #                             database = client[DATABASE_NAME]
        #                             collection = database[COLLECTION_NAME]
        #                             response = collection.insert_one(page_dict)
        #                             logging.info(f"Documento inserido com ID: {response.inserted_id} para URL: {url}")

        #                             # Remover o diretório do snapshot
        #                             try:
        #                                 shutil.rmtree(snapshot_dir)
        #                                 logging.info(f"Diretório {snapshot_dir} removido com sucesso.")
        #                             except Exception as rmtree_error:
        #                                 error_message = (
        #                                     f"Erro ao remover {snapshot_dir} para URL: {url} - {rmtree_error}\n"
        #                                 )
        #                                 logging.error(error_message)
        #                                 with open(error_log_path, 'a', encoding='utf-8') as ef:
        #                                     ef.write(error_message)

        #                             # Registrar o sucesso
        #                             with open(success_log_path, 'a', encoding='utf-8') as sf:
        #                                 sf.write(f"{url}\n")
        #                             logging.info(f"URL {url} registrada com sucesso.")
        #                     else:
        #                         error_message = f"Erro na extração do timestamp para URL: {url}\n"
        #                         logging.error(error_message)
        #                         with open(error_log_path, 'a', encoding='utf-8') as ef:
        #                             ef.write(error_message)
        #             else:
        #                 error_message = f"singlefile.html não encontrado para URL: {url}\n"
        #                 logging.error(error_message)
        #                 with open(error_log_path, 'a', encoding='utf-8') as ef:
        #                     ef.write(error_message)

        #         except Exception as e_individual:
        #             error_message = f"Erro processando URL {url} no chunk: {e_individual}\n"
        #             logging.error(error_message)
        #             with open(error_log_path, 'a', encoding='utf-8') as ef:
        #                 ef.write(error_message)

        #     # Se chegou até aqui, concluímos o lote com sucesso (ou com avisos).
        #     break        

        except subprocess.CalledProcessError as e:
            stderr_lower = e.stderr.lower() if e.stderr else ""
            if "database is locked" in stderr_lower:
                attempt += 1
                if attempt < RETRIES:
                    logging.warning(
                        f"database locked para esse chunk, esperando {DELAY}s e tentando novamente "
                        f"({attempt}/{RETRIES})"
                    )
                    time.sleep(DELAY)
                else:
                    msg = (
                        f"database locked após {RETRIES} tentativas para esse chunk. Abortando.\n"
                        f"URLs do chunk: {urls_chunk}"
                    )
                    logging.error(msg)
                    with open(error_log_path, "a", encoding="utf-8") as ef:
                        ef.write(msg + "\n")
            else:
                # Outro erro
                msg = f"Erro no subprocesso ArchiveBox: {e.stderr}\nChunk: {urls_chunk}"
                logging.error(msg)
                with open(error_log_path, "a", encoding="utf-8") as ef:
                    ef.write(msg + "\n")
                break

        except Exception as ex:
            ex_str = str(ex).lower()
            if "database is locked" in ex_str:
                attempt += 1
                if attempt < RETRIES:
                    logging.warning(
                        f"database locked (Exceção) para chunk, esperando {DELAY}s e tentando novamente "
                        f"({attempt}/{RETRIES})"
                    )
                    time.sleep(DELAY)
                else:
                    msg = (
                        f"database locked após {RETRIES} tentativas (Exceção). Abortando.\n"
                        f"URLs do chunk: {urls_chunk}"
                    )
                    logging.error(msg)
                    with open(error_log_path, "a", encoding="utf-8") as ef:
                        ef.write(msg + "\n")
            else:
                msg = f"Erro inesperado para chunk: {urls_chunk} - {ex}"
                logging.error(msg)
                with open(error_log_path, "a", encoding="utf-8") as ef:
                    ef.write(msg + "\n")
                break

# Conectar ao MongoDB (o client é thread-safe)
client = conectarBanco()

def main():
    if len(sys.argv) < 2:
        print(f"Uso: python {sys.argv[0]} <DOMÍNIO_OU_URL>")
        sys.exit(1)

    alvo = sys.argv[1].strip()
    logging.info(f"Iniciando coleta de capturas do Wayback Machine para: {alvo}")

    # 1) Habilitar WAL antes de começar (ajuda em gravações concorrentes)
    enable_wal_mode(os.path.join(ARCHIVEBOX_DIR, "index.sqlite3"))

    #2) Obter todos os snapshots via CDX
    snapshots = get_wayback_snapshots(alvo)
    if not snapshots:
       logging.info("Nenhum snapshot obtido. Encerrando.")
       return

    # 3) Salvar em um arquivo local (opcional, mas útil p/ referência)
    all_urls_file = os.path.join(ARCHIVEBOX_DIR, f"urls_list_func_singlefile.txt")
    save_urls_to_file(snapshots, all_urls_file)

    # 4) Carregar (de volta) as URLs e filtrar as já processadas
    all_urls = load_urls_from_file(all_urls_file)
    if not all_urls:
        logging.info("Lista de URLs vazia após leitura.")
        return

    success_log_path = Path(ARCHIVEBOX_DIR) / "success_wayback_urls.txt"
    if success_log_path.exists():
        already_done = set(load_urls_from_file(str(success_log_path)))
    else:
        already_done = set()

    to_process = [u for u in all_urls if u not in already_done]
    logging.info(f"Há {len(to_process)} URLs restantes para processar no ArchiveBox.")
    if not to_process:
        logging.info("Todas as URLs já foram processadas anteriormente.")
        return

    # 5) Quebrar as URLs em chunks (lotes)
    chunks = [to_process[i : i + CHUNK_SIZE] for i in range(0, len(to_process), CHUNK_SIZE)]
    logging.info(f"Serão gerados {len(chunks)} chunks de até {CHUNK_SIZE} URLs cada.")

    # 6) Processar em paralelo usando ThreadPoolExecutor
    logging.info(f"Processando em paralelo com max_workers={MAX_WORKERS}")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {executor.submit(archive_urls_chunk, chunk): chunk for chunk in chunks}
        for future in as_completed(future_map):
            chunk = future_map[future]
            try:
                future.result()  # Tenta pegar o resultado; se deu erro, lança exceção
            except Exception as e:
                logging.error(f"Erro processando chunk {chunk}: {e}")

    logging.info("Processo concluído com sucesso!")

if __name__ == "__main__":
    main()
