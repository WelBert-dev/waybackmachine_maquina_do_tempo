import subprocess
import os
import sys
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from datetime import datetime, timezone
import logging
import hashlib
from pathlib import Path
import re
from bson.binary import Binary
import shutil  # Importar shutil
import connect_local  # Certifique-se de que este módulo está corretamente configurado
import concurrent.futures

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

# Número máximo de threads concorrentes
MAX_WORKERS = 5  # Ajuste conforme os recursos do seu sistema

# Configurar o logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
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
            "advertising_id_when_isModified": self.advertising_id_when_isModified
        }

def extract_wayback_timestamp_substring(url: str) -> str:
    """
    Extrai o timestamp do Wayback Machine (YYYYMMDDhhmmss) de uma URL,
    utilizando substring (slicing). Retorna apenas a string do timestamp.
    """
    marker = "/web/"  # Ajuste conforme a estrutura real da URL
    try:
        # Encontra o índice inicial do trecho "/web/"
        start_index = url.index(marker) + len(marker)  # Posição logo após '/web/'
    except ValueError:
        # Se não encontrar "/web/", retorna None
        logging.error(f"Marker '/web/' não encontrado na URL: {url}")
        return None

    # Queremos os 14 caracteres após start_index
    timestamp_str = url[start_index : start_index + 14]

    # Verificar se realmente são 14 dígitos e a URL tem comprimento esperado
    if len(timestamp_str) == 14 and timestamp_str.isdigit() and len(url) == 67:
        return timestamp_str
    else:
        logging.error(f"Timestamp inválido extraído da URL: {url}")
        return None

def conectarBanco():
    """Estabelece a conexão com o MongoDB."""
    try:
        client = MongoClient(
            "mongodb://127.0.0.1:27017",  # Usando 127.0.0.1 conforme mencionado que funciona
            serverSelectionTimeoutMS=5000  # Tempo de espera de 5 segundos
        )
        # Força a verificação da conexão
        client.server_info()
        logging.info("Conexão ao MongoDB bem-sucedida!")
        return client
    except ServerSelectionTimeoutError as e:
        logging.error(f"Erro ao conectar ao MongoDB: {e}")
        return None

def calcular_hash(caminho_arquivo, algoritmo='md5'):
    """Calcula o hash do arquivo especificado."""
    try:
        hash_func = getattr(hashlib, algoritmo)()
        with open(caminho_arquivo, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_func.update(chunk)
        return hash_func.hexdigest()
    except Exception as e:
        logging.error(f"Erro ao calcular hash de {caminho_arquivo}: {e}")
        return None

def verificar_integridade(caminho_original, caminho_recuperado, algoritmo='md5'):
    """Compara os hashes dos arquivos original e recuperado para verificar integridade."""
    hash_original = calcular_hash(caminho_original, algoritmo)
    hash_recuperado = calcular_hash(caminho_recuperado, algoritmo)
    if hash_original and hash_recuperado:
        if hash_original == hash_recuperado:
            logging.info("Integridade verificada: os arquivos são idênticos.")
        else:
            logging.warning("Atenção: os arquivos diferem.")
    else:
        logging.error("Não foi possível calcular os hashes para verificação.")

def archive_url(url):
    """
    Arquiva uma URL usando o ArchiveBox, salva no MongoDB, realiza operações de recuperação,
    verifica a integridade dos dados e registra o sucesso ou erro em arquivos de texto.
    """
    # Definir os caminhos para os arquivos de log
    success_log = Path(ARCHIVEBOX_DIR) / "success_insertInto_mongo.txt"
    error_log = Path(ARCHIVEBOX_DIR) / "error_insertInto_mongo.txt"

    try:
        # Executar o comando ArchiveBox
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

        logging.info(f'ArchiveBox executado com sucesso para URL: {url}')

        # Exibir a saída completa do comando para depuração
        output = result.stdout
        logging.debug(f"Saída do ArchiveBox para URL {url}: {output}")

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
                    logging.info(f'Singlefile baixado: {singlefile_html}')

                    timestamp_str = extract_wayback_timestamp_substring(url)

                    if timestamp_str is not None:
                        # Converte a string para um objeto datetime "naive" (sem timezone)
                        dt_naive = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")

                        # Atribui o timezone UTC
                        dt_utc = dt_naive.replace(tzinfo=timezone.utc)

                        logging.info(f"Objeto datetime em UTC: {dt_utc}")
                        logging.info(f"Timestamp string: {timestamp_str}")

                        html_content = f.read()

                        # Verificar se o conteúdo foi lido corretamente
                        if not html_content:
                            error_message = f"Conteúdo HTML vazio para URL: {url}\n"
                            logging.error("Aviso: o conteúdo HTML está vazio.")
                            # Registrar o erro
                            with open(error_log, 'a', encoding='utf-8') as ef:
                                ef.write(error_message)
                        else:
                            # Criar o modelo de documento
                            page_model = ArquivosDaHomeWaybackMachineModel(
                                device='--window-size=1280,720',
                                content=html_content,
                                timestamp=dt_utc,
                                isAdvertisingModified=False,
                                advertising_id_when_isModified=None
                            )
                            page_dict = page_model.to_dict()

                            # Depuração: Imprimir o dicionário antes da inserção
                            logging.debug(f"Dicionário do documento a ser inserido para URL {url}: {page_dict}")

                            # Conectar ao MongoDB
                            client = conectarBanco()
                            if not client:
                                error_message = f"Falha na conexão com o MongoDB para URL: {url}\n"
                                logging.error("Não foi possível conectar ao MongoDB. Abortando operações adicionais.")
                                # Registrar o erro
                                with open(error_log, 'a', encoding='utf-8') as ef:
                                    ef.write(error_message)
                                return

                            database = client[DATABASE_NAME]
                            collection = database[COLLECTION_NAME]

                            # Inserir o documento no MongoDB
                            response = collection.insert_one(page_dict)
                            logging.info(f'Response do banco para URL {url}: {response.inserted_id}')

                            # Verificar se a inserção foi bem-sucedida
                            if response.inserted_id:
                                logging.info(f"Documento inserido com sucesso no MongoDB para URL: {url}")

                                # Recuperar os documentos inseridos pelo timestamp
                                documentos = list(collection.find({'timestamp': dt_utc}))
                                if documentos:
                                    for idx, documento in enumerate(documentos, start=1):
                                        logging.info(f"\nProcessando documento {idx}/{len(documentos)} para URL: {url}")

                                        # Depuração: Imprimir o documento recuperado
                                        logging.debug(f"Documento recuperado para URL {url}: {documento}")

                                        # Decodificar o conteúdo
                                        binary_content = documento.get('content')
                                        logging.debug(f"Tipo de 'content' para URL {url}: {type(binary_content)}")

                                        if binary_content and isinstance(binary_content, (bytes, Binary)):
                                            try:
                                                html_recuperado = binary_content.decode('utf-8')
                                                logging.info(f"Conteúdo decodificado com sucesso para URL: {url}")

                                                # Definir o caminho para salvar o HTML recuperado
                                                caminho_recuperado = snapshot_dir / f"recuperado_{timestamp_str}.html"

                                                # Salvar o HTML recuperado
                                                with open(caminho_recuperado, 'w', encoding='utf-8') as recuperado_file:
                                                    recuperado_file.write(html_recuperado)
                                                logging.info(f"Arquivo HTML recuperado salvo em: {caminho_recuperado}")

                                                # Verificar a integridade comparando com o original
                                                verificar_integridade(str(singlefile_html), str(caminho_recuperado))

                                                # **Excluir o diretório snapshot_dir recursivamente**
                                                try:
                                                    shutil.rmtree(snapshot_dir)
                                                    logging.info(f"Diretório {snapshot_dir} excluído com sucesso para URL: {url}.")
                                                except Exception as rmtree_error:
                                                    error_message = f"Erro ao excluir o diretório {snapshot_dir} para URL: {url} - {rmtree_error}\n"
                                                    logging.error(f"Erro ao excluir o diretório {snapshot_dir} para URL: {url}: {rmtree_error}")
                                                    # Registrar o erro
                                                    with open(error_log, 'a', encoding='utf-8') as ef:
                                                        ef.write(error_message)

                                                # **Registrar o sucesso**
                                                success_message = f"{url}\n"
                                                with open(success_log, 'a', encoding='utf-8') as sf:
                                                    sf.write(success_message)
                                                logging.info(f"URL registrada em {success_log}: {url}")

                                            except Exception as decode_error:
                                                error_message = f"Erro ao decodificar o conteúdo para URL: {url} - {decode_error}\n"
                                                logging.error(f"Erro ao decodificar o conteúdo para URL: {url}: {decode_error}")
                                                # Registrar o erro
                                                with open(error_log, 'a', encoding='utf-8') as ef:
                                                    ef.write(error_message)
                                        else:
                                            error_message = f"Campo 'content' não encontrado ou não está no formato esperado (Binary ou bytes) para URL: {url}\n"
                                            logging.error(f"Campo 'content' não encontrado ou não está no formato esperado para URL: {url}.")
                                            # Registrar o erro
                                            with open(error_log, 'a', encoding='utf-8') as ef:
                                                ef.write(error_message)
                                else:
                                    error_message = f"Nenhum documento encontrado para o timestamp {dt_utc} na URL: {url}\n"
                                    logging.error(f"Nenhum documento encontrado para o timestamp {dt_utc} na URL: {url}.")
                                    # Registrar o erro
                                    with open(error_log, 'a', encoding='utf-8') as ef:
                                        ef.write(error_message)
                            else:
                                error_message = f"Falha na inserção do documento no MongoDB para URL: {url}\n"
                                logging.error(f"Falha na inserção do documento no MongoDB para URL: {url}.")
                                # Registrar o erro
                                with open(error_log, 'a', encoding='utf-8') as ef:
                                    ef.write(error_message)
            else:
                error_message = f"Arquivo singlefile_html não encontrado em {snapshot_dir} para URL: {url}\n"
                logging.warning(f"Arquivo singlefile_html não encontrado em {snapshot_dir} para URL: {url}.")
                # Registrar o erro
                with open(error_log, 'a', encoding='utf-8') as ef:
                    ef.write(error_message)
        else:
            error_message = f"Não foi possível encontrar o caminho do snapshot na saída para URL: {url}\n"
            logging.warning(f"Não foi possível encontrar o caminho do snapshot na saída do ArchiveBox para URL: {url}.")
            # Registrar o erro
            with open(error_log, 'a', encoding='utf-8') as ef:
                ef.write(error_message)
    except subprocess.CalledProcessError as e:
        error_message = f"Erro ao arquivar {url}: {e.stderr}\n"
        logging.error(f"Erro ao executar o comando ArchiveBox para URL: {url}: {e}")
        logging.error(f"Saída de erro: {e.stderr}")
        # Registrar o erro
        with open(error_log, 'a', encoding='utf-8') as ef:
            ef.write(error_message)
    except json.JSONDecodeError as e:
        error_message = f"Não foi possível decodificar o JSON retornado pelo ArchiveBox para URL: {url} - {e}\n"
        logging.error(f"Não foi possível decodificar o JSON retornado pelo ArchiveBox para URL: {url}: {e}")
        # Registrar o erro
        with open(error_log, 'a', encoding='utf-8') as ef:
            ef.write(error_message)
    except KeyError as e:
        error_message = f"Campo ausente no JSON do ArchiveBox para URL: {url} - {e}\n"
        logging.error(f"Campo ausente no JSON do ArchiveBox para URL: {url}: {e}")
        # Registrar o erro
        with open(error_log, 'a', encoding='utf-8') as ef:
            ef.write(error_message)
    except Exception as ex:
        error_message = f"Ocorreu um erro inesperado para URL: {url} - {ex}\n"
        logging.error(f"Ocorreu um erro inesperado para URL: {url}: {ex}")
        # Registrar o erro
        with open(error_log, 'a', encoding='utf-8') as ef:
            ef.write(error_message)

def main():
    """
    Função principal que lê as URLs de um arquivo, filtra aquelas que já foram processadas com sucesso,
    e arquiva as URLs restantes.
    """
    logging.info("Iniciando o processo de arquivamento...")

    # Caminho completo para o arquivo de URLs
    urls_file_path = Path(URL_LIST_FILE)
    if not urls_file_path.exists():
        logging.error(f"Arquivo {URL_LIST_FILE} não encontrado em {ARCHIVEBOX_DIR}.")
        sys.exit(1)

    # Ler as URLs do arquivo, ignorando linhas vazias e comentários
    with open(urls_file_path, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]

    if not urls:
        logging.warning("Nenhuma URL encontrada para arquivar.")
        sys.exit(0)

    # Caminhos para os arquivos de log
    success_log = Path(ARCHIVEBOX_DIR) / "success_insertInto_mongo.txt"
    error_log = Path(ARCHIVEBOX_DIR) / "error_insertInto_mongo.txt"

    # Ler as URLs já processadas com sucesso
    if success_log.exists():
        with open(success_log, "r", encoding="utf-8") as sf:
            processed_urls = set(line.strip() for line in sf if line.strip())
        logging.info(f"{len(processed_urls)} URLs já foram processadas com sucesso.")
    else:
        processed_urls = set()
        logging.info("Nenhuma URL processada anteriormente.")

    # Filtrar URLs que já foram processadas com sucesso
    urls_to_process = [url for url in urls if url not in processed_urls]

    if not urls_to_process:
        logging.info("Todas as URLs já foram processadas com sucesso.")
        sys.exit(0)

    logging.info(f"{len(urls_to_process)} URLs serão processadas.")

    # Conectar ao MongoDB antecipadamente
    client = conectarBanco()
    if not client:
        logging.error("Não foi possível conectar ao MongoDB. Abortando o processo.")
        sys.exit(1)

    # Definir uma função auxiliar para arquivar uma única URL com a conexão MongoDB
    def process_url(url):
        archive_url(url)

    # Utilizar ThreadPoolExecutor para processar múltiplas URLs simultaneamente
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Mapeia as URLs para os threads
        future_to_url = {executor.submit(process_url, url): url for url in urls_to_process}

        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                future.result()
            except Exception as exc:
                logging.error(f"URL {url} gerou uma exceção: {exc}")

    logging.info("Processo de arquivamento concluído.")

if __name__ == "__main__":
    main()
