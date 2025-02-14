#!/bin/zsh

# Script para configurar o ArchiveBox

# Cores para saída
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'  # Sem cor

echo "\n\n\n${GREEN}Iniciando configuração do ArchiveBox...${NC}"

# Verificar e instalar 'expect', se necessário
if ! command -v expect >/dev/null 2>&1; then
    echo "\n\n\n${RED}Expect não encontrado. Instalando com Homebrew...${NC}"
    brew install expect
else
    echo "\n\n\n${GREEN}Expect já está instalado.${NC}"
fi

# Remover e criar o diretório 'archivebox' apenas se necessário
if [ -d "archivebox" ]; then
    echo "\n\n\n${RED}'archivebox' já existe. Removendo...${NC}"
    rm -rf archivebox
fi

echo "\n\n\n${GREEN}Criando diretório 'archivebox'...${NC}"
mkdir -p archivebox
chmod 777 archivebox

cd archivebox || exit

# Exigir que o pyenv esteja instalado para gerenciar o Python
if ! command -v pyenv >/dev/null 2>&1; then
    echo "\n\n\n${RED}Pyenv não encontrado. Instalando pyenv...${NC}"
    brew install pyenv
    echo 'eval "$(pyenv init -)"' >> ~/.zshrc
    source ~/.zshrc
fi

# Configurar pyenv para utilizar Python 3.11.5
echo "\n\n\n${GREEN}Definindo Python 3.11.5 como versão global com pyenv...${NC}"
pyenv install 3.11.5 --skip-existing
pyenv global 3.11.5
# Inicializar os shims do pyenv no shell atual
eval "$(pyenv init -)"

# Obter o caminho completo do Python instalado pelo pyenv
PYTHON=$(pyenv which python)
if [ -z "$PYTHON" ]; then
    echo "\n\n\n${RED}Erro: Python não encontrado via pyenv.${NC}"
    exit 1
fi
echo "\n\n\n${GREEN}Python configurado via pyenv: $($PYTHON --version)${NC}"

# Criar ambiente virtual utilizando o Python instalado pelo pyenv
if [ ! -d ".venv" ]; then
    echo "\n\n\n${GREEN}Criando ambiente virtual Python...${NC}"
    $PYTHON -m venv .venv
    echo "\n\n\n${GREEN}Ambiente virtual criado.${NC}"
else
    echo "\n\n\n${RED}Ambiente virtual já existe.${NC}"
fi

# Ativar ambiente virtual
echo "\n\n\n${GREEN}Ativando o ambiente virtual...${NC}"
source .venv/bin/activate

# Verificar e instalar o pip, se necessário
if ! command -v pip >/dev/null 2>&1; then
    echo "\n\n\n${RED}Pip não encontrado. Instalando pip...${NC}"
    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
    $PYTHON get-pip.py
    rm get-pip.py
    echo "\n\n\n${GREEN}Pip instalado com sucesso.${NC}"
else
    echo "\n\n\n${GREEN}Pip já está instalado.${NC}"
fi

# Atualizar pip
echo "\n\n\n${GREEN}Atualizando pip...${NC}"
pip install --upgrade pip

# Instalar ArchiveBox, pymongo e playwright, se necessário
echo "\n\n\n${GREEN}Instalando ArchiveBox...${NC}"
pip install --upgrade archivebox && echo "${GREEN}ArchiveBox instalado com sucesso.${NC}" || echo "${RED}Erro ao instalar ArchiveBox.${NC}"

echo "\n\n\n${GREEN}Instalando pymongo...${NC}"
pip install pymongo && echo "${GREEN}Pymongo instalado com sucesso.${NC}" || echo "${RED}Erro ao instalar pymongo.${NC}"

echo "\n\n\n${GREEN}Instalando Playwright...${NC}"
pip install playwright && echo "${GREEN}Playwright instalado com sucesso.${NC}" || echo "${RED}Erro ao instalar Playwright.${NC}"

# Criar e entrar no diretório 'get'
if [ ! -d "get" ]; then
    echo "\n\n\n${GREEN}Criando o diretório 'get'...${NC}"
    mkdir get
else
    echo "\n\n\n${RED}Diretório 'get' já existe.${NC}"
fi

cd get || exit

# Garantir que o NVM esteja instalado e carregado
if ! command -v nvm >/dev/null 2>&1; then
    echo "\n\n\n${RED}NVM não encontrado. Instalando NVM...${NC}"
    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.3/install.sh | bash
    export NVM_DIR="$HOME/.nvm"
    [ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
    [ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"
fi

# Verificar novamente se o NVM está disponível
if command -v nvm >/dev/null 2>&1; then
    echo "\n\n\n${GREEN}Instalando Node.js versão 18 com NVM...${NC}"
    nvm install 18 && nvm use 18 && echo "${GREEN}Node.js 18 instalado com sucesso.${NC}" || echo "${RED}Erro ao instalar Node.js 18.${NC}"
else
    echo "\n\n\n${RED}NVM ainda não está disponível. Pule esta etapa.${NC}"
fi

# Inicializar ArchiveBox, se necessário
if [ ! -f "index.sqlite3" ]; then
    echo "\n\n\n${GREEN}Inicializando ArchiveBox...${NC}"
    touch index.sqlite3
    chmod 777 index.sqlite3
    archivebox init && echo "${GREEN}ArchiveBox inicializado com sucesso.${NC}" || echo "${RED}Erro ao inicializar ArchiveBox.${NC}"
else
    echo "\n\n\n${RED}ArchiveBox já está inicializado.${NC}"
fi

# Automatizar configuração do ArchiveBox com entradas automáticas
echo "\n\n\n${GREEN}Configurando ArchiveBox com entradas automáticas...${NC}"
expect << EOF
spawn archivebox setup
expect "Username (leave blank to use*" { send "wellison\r" }
expect "Email address:" { send "wellison.bertelli@hotmail.com\r" }
expect "Password:" { send "minhasenha123\r" }
expect "Password (again):" { send "minhasenha123\r" }
expect "Bypass password validation and create user anyway\? \[y/N\]:" { send "y\r" }
expect eof
EOF

# Instalar SingleFile CLI versão 1.1.54 globalmente via NPM
if ! npm list -g single-file-cli@1.1.54 >/dev/null 2>&1; then
    echo "\n\n\n${GREEN}Instalando SingleFile CLI versão 1.1.54...${NC}"
    npm install -g single-file-cli@1.1.54 && echo "${GREEN}SingleFile CLI instalado com sucesso.${NC}" || echo "${RED}Erro ao instalar SingleFile CLI.${NC}"
else
    echo "\n\n\n${GREEN}SingleFile CLI versão 1.1.54 já está instalado.${NC}"
fi

# Configurando opções do ArchiveBox
echo "\n\n\n${GREEN}Configurando opções do ArchiveBox...${NC}"
archivebox config --set SAVE_WGET=False
archivebox config --set SAVE_PDF=False
archivebox config --set SAVE_GIT=False
archivebox config --set USE_GIT=False
archivebox config --set SAVE_MEDIA=False
archivebox config --set USE_YOUTUBEDL=False
archivebox config --set SAVE_READABILITY=False
archivebox config --set USE_READABILITY=False
archivebox config --set SAVE_MERCURY=False
archivebox config --set USE_MERCURY=False
archivebox config --set SAVE_TITLE=False
archivebox config --set SAVE_FAVICON=False
archivebox config --set SAVE_WARC=False
archivebox config --set USE_WGET=False
archivebox config --set SAVE_SCREENSHOT=False
archivebox config --set SAVE_ARCHIVE_DOT_ORG=False
archivebox config --set USE_CURL=False
archivebox config --set SAVE_HTMLTOTEXT=False
archivebox config --set SAVE_HEADERS=False
archivebox config --set SAVE_DOM=False
archivebox config --set CHROME_HEADLESS=True
archivebox config --set TIMEOUT=240
archivebox config --set CHROME_BINARY="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

echo "\n\n\n${GREEN}Configuração concluída com sucesso!${NC}"
