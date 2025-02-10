import subprocess
import os
import sys
import sqlite3
from pymongo import MongoClient
from datetime import datetime, timedelta
import logging
import asyncio
from playwright.async_api import async_playwright
import re

# Configurações
ARCHIVEBOX_DIR = "/Users/wellisonbertelli/Documents/Poder360_estagio/waybackmachine_maquina_do_tempo/archivebox/get"  # Substitua pelo caminho correto
URL_LIST_FILE = "urls_list_test.txt"  # Arquivo contendo as URLs
MONGODB_URI = "mongodb://localhost:27017/"  # Substitua conforme necessário
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

class ArquivosDaHomeNovosObtidosComSeleniumModel:
    def __init__(self, device, content, timestamp, isAdvertisingModified, advertising_id_when_isModified):
        self.device = device
        self.content = content
        self.timestamp = timestamp
        self.isAdvertisingModified = isAdvertisingModified
        self.advertising_id_when_isModified = advertising_id_when_isModified

    def to_dict(self):
        return {
            "device": self.device,
            "content": self.content,
            "timestamp": self.timestamp,
            "isAdvertisingModified": self.isAdvertisingModified,
            "advertising_id_when_isModified": self.advertising_id_when_isModified
        }

def archive_url(url):
    """Executa o comando 'archivebox add' para uma URL específica."""
    try:
        result = subprocess.run(
            ["archivebox", "add", url, "--debug"],
            cwd=ARCHIVEBOX_DIR,
            capture_output=True,
            text=True,
            check=True
        )
        logging.info(f"Sucesso ao arquivar: {url}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Erro ao arquivar {url}: {e.stderr}")

def fetch_archived_entries(recent_hours=24):
    """Busca entradas arquivadas no index.sqlite3 adicionadas nas últimas X horas."""
    conn = sqlite3.connect(ARCHIVEBOX_INDEX_DB)
    cursor = conn.cursor()

    # # (1) Listar as tabelas disponíveis
    # cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    # tables = cursor.fetchall()
    
    # # (2) Exibir as tabelas no console para debug
    # print("Tabelas disponíveis no banco de dados:", tables)
    
    # # Calcular o timestamp de corte
    # cutoff_time = datetime.utcnow() - timedelta(hours=recent_hours)
    # cutoff_timestamp = cutoff_time.timestamp()
    
    # cursor.execute("""
    #     SELECT core_snapshot.id, core_snapshot.title, core_snapshot.url, core_snapshot.timestamp
    #     FROM core_snapshot
    #     WHERE core_snapshot.timestamp >= ?
    #     ORDER BY core_snapshot.id DESC
    # """, (cutoff_timestamp,))

    cursor.execute("""
        SELECT core_snapshot.id, core_snapshot.title, core_snapshot.url, core_snapshot.timestamp
        FROM core_snapshot
        ORDER BY core_snapshot.id DESC
    """)    
    
    rows = cursor.fetchall()

    # # (4) Imprimir as linhas retornadas
    # print("Linhas retornadas pela query:", rows)

    # # Obtém a lista de tabelas do banco
    # cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    # tables = [row[0] for row in cursor.fetchall()]

    # for table in tables:
    #     print(f"\n=== Tabela: {table} ===")

    #     # 1) Obter as colunas da tabela
    #     cursor.execute(f"PRAGMA table_info({table})")
    #     columns_info = cursor.fetchall()
    #     # columns_info é uma lista de tuplas: (col_id, col_name, col_type, ..., ...)
    #     columns_names = [col[1] for col in columns_info]
    #     print("Colunas:", columns_names)

    #     # 2) Obter os 5 primeiros registros (head)
    #     cursor.execute(f"SELECT * FROM {table} LIMIT 5")
    #     rows = cursor.fetchall()
    #     print("Primeiros 5 registros:")
    #     for i, row in enumerate(rows, start=1):
    #         print(f"  {i}. {row}")

    conn.close()
    
    # Converter os dados para uma lista de objetos de modelo
    archived_entries = []
    for row in rows:
        snapshot_id, title, url, timestamp = row
        # Extraia o timestamp do Wayback Machine URL
        dt = datetime.utcfromtimestamp(float(timestamp))
        iso_timestamp = dt.isoformat() + 'Z'

        print(f"\n\n\niso_timestamp: {iso_timestamp}")
        if not iso_timestamp:
            logging.warning(f"Timestamp inválido para snapshot ID {timestamp}, ignorando entrada.")
            continue
        
        archive_link = f"http://0.0.0.0:8000/archive/{timestamp}/index.html#original"
        
        # Criar uma instância do modelo
        entry = ArquivosDaHomeNovosObtidosComSeleniumModel(
            device="--window-size=1280,720",
            content=archive_link,  # Inicialmente, armazenamos o link; será substituído pelo conteúdo HTML
            timestamp=iso_timestamp,
            isAdvertisingModified=False,
            advertising_id_when_isModified=None
        )
        
        archived_entries.append(entry)
    
    return archived_entries

async def fetch_full_html(url):
    """Usa Playwright para obter o HTML completo da página com recursos embutidos."""
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(url, timeout=60000)  # Timeout de 60 segundos
            await page.wait_for_load_state('networkidle')  # Espera até que a rede esteja ociosa
            content = await page.content()
            await browser.close()
            return content
    except Exception as e:
        logging.error(f"Erro ao obter conteúdo de {url}: {e}")
        return None

def create_consolidated_index(archived_data, output_path):
    """Cria um arquivo index.html consolidado contendo todas as páginas arquivadas."""
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("<!DOCTYPE html>\n<html lang='pt-BR'>\n<head>\n<meta charset='UTF-8'>\n")
        f.write("<meta name='viewport' content='width=device-width, initial-scale=1.0'>\n")
        f.write("<title>Índice Consolidado ArchiveBox</title>\n")
        # Adicionar estilos básicos
        f.write("""
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            h1 { text-align: center; }
            .archive-entry { margin-bottom: 20px; }
            .archive-entry h2 { margin-bottom: 5px; }
            .archive-entry a { text-decoration: none; color: #1a0dab; }
            .archive-entry a:hover { text-decoration: underline; }
            .archived-on { color: #555; font-size: 0.9em; }
        </style>
        """)
        f.write("</head>\n<body>\n")
        f.write("<h1>Índice Consolidado ArchiveBox</h1>\n")
    
        for entry in archived_data:
            f.write("<div class='archive-entry'>\n")
            f.write(f"  <h2><a href='{entry['url']}' target='_blank'>Snapshot ID: {entry['url'].split('/')[-2]}</a></h2>\n")
            f.write(f"  <p class='archived-on'>Arquivado em: {entry['timestamp']}</p>\n")
            f.write("</div>\n")
    
        f.write("</body>\n</html>")
    logging.info(f"Arquivo index.html consolidado criado em {output_path}")

async def process_archived_entries(archived_entries):
    """Processa cada entrada arquivada para obter o HTML completo e atualizar o conteúdo."""
    for entry in archived_entries:
        logging.info(f"Processando snapshot content {entry.content}")
        html_content = await fetch_full_html(entry.content)
        if html_content:
            entry.content = html_content
            logging.info(f"Conteúdo obtido para snapshot content {entry.content}")
        else:
            logging.error(f"Falha ao obter conteúdo para snapshot content {entry.content}")

def upload_to_mongodb(archived_data):
    """Envia os dados arquivados para o MongoDB."""
    try:
        client = MongoClient(MONGODB_URI)
        database = client[DATABASE_NAME]
        collection = database[COLLECTION_NAME]
        
        # Converter objetos para dicionários
        docs = [entry.to_dict() for entry in archived_data]
        
        # Inserir dados
        if docs:
            # Opcional: Verificar duplicatas com base no timestamp e no conteúdo
            # Por exemplo, utilizar timestamp como campo único ou criar um índice único no MongoDB
            collection.insert_many(docs)
            logging.info(f"{len(docs)} documentos inseridos no MongoDB.")
        else:
            logging.info("Nenhum dado para inserir no MongoDB.")
    except Exception as e:
        logging.error(f"Erro ao inserir no MongoDB: {e}")

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
    
    # Buscar as entradas arquivadas nas últimas 24 horas
    archived_entries = fetch_archived_entries(recent_hours=24)
    
    if not archived_entries:
        logging.warning("Nenhuma entrada arquivada recentemente para processar.")
        sys.exit(0)
    
    # Processar as entradas arquivadas para obter o HTML completo
    asyncio.run(process_archived_entries(archived_entries))
    
    # Criar o arquivo index.html consolidado
    consolidated_index_path = os.path.join(ARCHIVEBOX_DIR, "consolidated_index.html")
    create_consolidated_index(archived_entries, consolidated_index_path)
    
    # Enviar os dados para o MongoDB
    upload_to_mongodb(archived_entries)

if __name__ == "__main__":
    main()
