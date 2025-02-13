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

# =======================================
# CONFIGURAÇÕES
# =======================================
WAYBACK_CDX_API = "http://web.archive.org/cdx/search/cdx"
ARCHIVEBOX_DIR = "/Users/wellisonbertelli/waybackmachine_maquina_do_tempo/archivebox/get"  # Substitua pelo caminho correto
CHUNK_SIZE = 10  # Quantas URLs por subprocess do ArchiveBox
RETRIES = 3      # Número de tentativas em caso de "database locked"
DELAY = 5        # Tempo (s) de espera entre tentativas
LOG_FILE = os.path.join(ARCHIVEBOX_DIR, "wayback_download_generica_outro_prompt.log")

# Configurar o logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

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
        "from": "1996",        # ano inicial
        "to": "2025",          # ano final (ou algo mais atual)
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
        wayback_url = f"https://web.archive.org/web/{timestamp}/{original_url}"
        snapshots.append(wayback_url)

    logging.info(f"Foram encontradas {len(snapshots)} capturas no CDX.")
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
            # Registrar sucesso individual
            with open(success_log_path, "a", encoding="utf-8") as sf:
                for u in urls_chunk:
                    sf.write(u + "\n")
            break

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

def main():
    if len(sys.argv) < 2:
        print(f"Uso: python {sys.argv[0]} <DOMÍNIO_OU_URL>")
        sys.exit(1)

    alvo = sys.argv[1].strip()
    logging.info(f"Iniciando coleta de capturas do Wayback Machine para: {alvo}")

    # 1) Habilitar WAL antes de começar (ajuda em gravações concorrentes)
    enable_wal_mode(os.path.join(ARCHIVEBOX_DIR, "index.sqlite3"))

    # 2) Obter todos os snapshots via CDX
    snapshots = get_wayback_snapshots(alvo)
    if not snapshots:
        logging.info("Nenhum snapshot obtido. Encerrando.")
        return

    # 3) Salvar em um arquivo local (opcional, mas útil p/ referência)
    all_urls_file = os.path.join(ARCHIVEBOX_DIR, f"all_snapshots_{alvo.replace('/', '_')}.txt")
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
    max_workers = 3  # Ajuste conforme sua capacidade e volume
    logging.info(f"Processando em paralelo com max_workers={max_workers}")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
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
