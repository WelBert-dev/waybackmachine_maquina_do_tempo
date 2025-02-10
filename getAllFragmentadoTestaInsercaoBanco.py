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
import connect_local

# # Configurações
# ARCHIVEBOX_DIR = "/Users/wellisonbertelli/Documents/Poder360_estagio/waybackmachine_maquina_do_tempo/archivebox/get"  # Substitua pelo caminho correto
# # ARCHIVEBOX_DIR = "."  # Substitua pelo caminho correto
# URL_LIST_FILE = "../../urls_list_test.txt"  # Arquivo contendo as URLs

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

    # def to_dict(self):
    #     return {
    #         "device": self.device,
    #         "content": self.content,
    #         "timestamp": self.timestamp,
    #         "isAdvertisingModified": self.isAdvertisingModified,
    #         "advertising_id_when_isModified": self.advertising_id_when_isModified
    #     }
    
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
    
def recuperar_documento_por_timestamp(client, database_name, collection_name, dt_utc):
    """
    Recupera documentos da coleção com o campo 'timestamp' igual a 'dt_utc'.

    :param client: Cliente MongoDB conectado.
    :param database_name: Nome do banco de dados.
    :param collection_name: Nome da coleção.
    :param dt_utc: Objeto datetime com timezone UTC.
    :return: Lista de documentos correspondentes.
    """
    db = client[database_name]
    collection = db[collection_name]
    try:
        # Realiza a consulta pelo campo 'timestamp'
        documentos = list(collection.find({'timestamp': dt_utc}))
        if documentos:
            print(f"{len(documentos)} documento(s) encontrado(s) para o timestamp {dt_utc}.")
            return documentos
        else:
            print(f"Nenhum documento encontrado para o timestamp {dt_utc}.")
            return []
    except Exception as e:
        print(f"Erro ao recuperar documento: {e}")
        return []
    
def decodificar_conteudo(documento):
    """
    Decodifica o campo 'content' do documento de Binary para string UTF-8.

    :param documento: Documento recuperado do MongoDB.
    :return: String contendo o conteúdo HTML ou None em caso de erro.
    """
    try:
        binary_content = documento.get('content')
        if binary_content:
            if isinstance(binary_content, Binary):
                html_content = binary_content.decode('utf-8')
                print("Conteúdo decodificado com sucesso.")
                return html_content
            else:
                print("Campo 'content' não está no formato esperado (Binary).")
                return None
        else:
            print("Campo 'content' não encontrado no documento.")
            return None
    except Exception as e:
        print(f"Erro ao decodificar o conteúdo: {e}")
        return None
    
def salvar_html(html_content, caminho_saida):
    """
    Salva a string HTML em um arquivo no caminho especificado.

    :param html_content: Conteúdo HTML em string.
    :param caminho_saida: Caminho completo onde o arquivo será salvo.
    """
    try:
        with open(caminho_saida, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"Arquivo HTML salvo em {caminho_saida}")
    except Exception as e:
        print(f"Erro ao salvar o arquivo HTML: {e}")

def archive_url(url):
    """Executa o comando 'archivebox add' para uma URL específica, 
    obtem o HTML recém-baixado e salva no banco de dados."""
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

                         # Agora, recuperar o documento pelo timestamp
                        documentos = list(collection.find({'timestamp': dt_utc}))

                        if documentos:
                            print(f"{len(documentos)} documento(s) encontrado(s) para o timestamp {dt_utc}.")

                            for idx, documento in enumerate(documentos, start=1):
                                print(f"\nProcessando documento {idx}/{len(documentos)}:")

                                # Decodificar o conteúdo
                                binary_content = documento.get('content')

                                print(f"Tipo de 'content': {type(binary_content)}")
                                
                                if binary_content and isinstance(binary_content, (bytes, Binary)):
                                    try:
                                        html_recuperado = binary_content.decode('utf-8')
                                        print("Conteúdo decodificado com sucesso.")

                                        # Definir o caminho para salvar o HTML recuperado
                                        # Exemplo: adicionar sufixo '_recuperado' ao nome original
                                        caminho_recuperado = snapshot_dir / f"recuperado_{timestamp_str}.html"

                                        # Salvar o HTML recuperado
                                        with open(caminho_recuperado, 'w', encoding='utf-8') as recuperado_file:
                                            recuperado_file.write(html_recuperado)
                                        print(f"Arquivo HTML recuperado salvo em: {caminho_recuperado}")

                                        # (Opcional) Verificar a integridade comparando com o original
                                        # caminho_original = singlefile_html
                                        # verificar_integridade(str(caminho_original), str(caminho_recuperado))

                                    except Exception as decode_error:
                                        print(f"Erro ao decodificar o conteúdo: {decode_error}")
                                else:
                                    print("Campo 'content' não encontrado ou não está no formato esperado (Binary).")
                        else:
                            print(f"Nenhum documento encontrado para o timestamp {dt_utc}.")


                        # page_dict = page_model.to_dict()

                        # database = client["database"]

                        # response = database["arquivos_da_home_wayback_machine"].insert_one(page_dict)

                        # print(f'HTML salvo como {timestamp}')
                        # print(f'Response do banco: {response}')

                        logging.info(f"Snapshot salvo no MongoDB: {snapshot_dir}")

                    else:
                        logging.error(f"Erro na extração do timestamp na URL :>> {url}")
            else:
                logging.warning(f"Arquivo singlefile_html não encontrado em {snapshot_dir}")

        else:
            logging.warning("Não foi possível encontrar o caminho do snapshot na saída.")
        
        # # Tenta carregar a saída JSON
        # data = json.loads(result.stdout)
        
        # # Normalmente, o JSON retornado tem um objeto "added" contendo informações
        # # sobre o snapshot criado. Exemplo simplificado:
        # #
        # # {
        # #   "added": [
        # #       {
        # #           "url": "http://example.com",
        # #           "base_path": "archive/1693329601.123456",
        # #           "timestamp": "1693329601.123456",
        # #           "title": "Example Domain",
        # #           ...
        # #       }
        # #   ],
        # #   ...
        # # }
        # #
        # # Obtenha o path do snapshot recém-criado
        # added = data.get("added", [])
        # if not added:
        #     logging.warning("Nenhum snapshot criado. Verifique a saída do ArchiveBox.")
        #     return
        
        # # Pega o primeiro snapshot (ou itere, se houver mais de um)
        # snapshot_info = added[0]
        # base_path = snapshot_info["base_path"]  # Ex: "archive/1693329601.123456"

        # # Caminho completo para o arquivo index.html
        # snapshot_dir = Path(ARCHIVEBOX_DIR) / base_path
        # index_file = snapshot_dir / "singlefile.html"
        
        # if not index_file.exists():
        #     logging.warning(f"Não foi encontrado index.html em {index_file}")
        #     return
        
        # # Lê o conteúdo HTML
        # with open(index_file, "r", encoding="utf-8") as f:
        #     html_content = f.read()

        #     print(html_content)
        
        # # Exemplo: salvando no banco (pode ser Django, sqlite manual, etc.)
        # # Aqui vamos supor uma model Django 'PaginaArquivada' de exemplo:
        # # PaginaArquivada.objects.create(
        # #     url=url,
        # #     content_html=html_content,
        # #     data_criacao=datetime.utcnow(),
        # # )
        # #
        # # OU, se estiver usando sqlite manual, use a lógica de INSERT via cursor.

        # logging.info(f"Snapshot salvo com sucesso para {url}. Caminho: {index_file}")

    except subprocess.CalledProcessError as e:
        logging.error(f"Erro ao arquivar {url}: {e.stderr}")
    except json.JSONDecodeError as e:
        logging.error(f"Não foi possível decodificar o JSON retornado pelo ArchiveBox: {e}")
    except KeyError as e:
        logging.error(f"Campo ausente no JSON do ArchiveBox: {e}")

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
        entry = ArquivosDaHomeWaybackMachineModel(
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
    
    # # Buscar as entradas arquivadas nas últimas 24 horas
    # archived_entries = fetch_archived_entries(recent_hours=24)
    
    # if not archived_entries:
    #     logging.warning("Nenhuma entrada arquivada recentemente para processar.")
    #     sys.exit(0)
    
    # # Processar as entradas arquivadas para obter o HTML completo
    # asyncio.run(process_archived_entries(archived_entries))
    
    # # Criar o arquivo index.html consolidado
    # consolidated_index_path = os.path.join(ARCHIVEBOX_DIR, "consolidated_index.html")
    # create_consolidated_index(archived_entries, consolidated_index_path)
    
    # # Enviar os dados para o MongoDB
    # upload_to_mongodb(archived_entries)

if __name__ == "__main__":
    main()
