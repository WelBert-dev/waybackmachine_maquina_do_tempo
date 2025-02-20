__package__ = 'archivebox.extractors'

import random
import json
from pathlib import Path
from typing import Optional

from ..index.schema import Link, ArchiveResult, ArchiveError
from ..system import run, chmod_file
from ..util import enforce_types, is_static_file, chrome_args
from ..config import (
    TIMEOUT,
    SAVE_SINGLEFILE,
    DEPENDENCIES,
    SINGLEFILE_VERSION,
    SINGLEFILE_ARGS,
    CHROME_BINARY,
)
from ..logging_util import TimedProgress


# ---------------------------------------------------
# EXEMPLO: URL de proxy ZenRows
# Substitua pela sua credencial/caminho, ex.:
# "http://SUA_API_KEY:js_render=true@proxy.zenrows.com:8001"
# ---------------------------------------------------
ZENROWS_PROXY = "http://2da451f28dab5aab6e670c5aa1fe7617da7c01a2:js_render=true@proxy.zenrows.com:8001"


def get_random_user_agent() -> str:
    """
    Gera um User-Agent aleatório para cada execução.
    Ajuste esta função conforme sua necessidade.
    """
    chrome_versions = [f"{major}.{minor}.0.0" for major in range(100, 125) for minor in range(0, 5)]
    platforms = [
        "Windows NT 10.0; Win64; x64",
        "Macintosh; Intel Mac OS X 10_15_7",
        "X11; Linux x86_64",
    ]
    platform = random.choice(platforms)
    chrome_version = random.choice(chrome_versions)
    return f"Mozilla/5.0 ({platform}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{chrome_version} Safari/537.36"


@enforce_types
def should_save_singlefile(link: Link, out_dir: Optional[Path]=None, overwrite: Optional[bool]=False) -> bool:
    if is_static_file(link.url):
        return False

    out_dir = out_dir or Path(link.link_dir)
    if not overwrite and (out_dir / 'singlefile.html').exists():
        return False

    return SAVE_SINGLEFILE


@enforce_types
def save_singlefile(link: Link, out_dir: Optional[Path]=None, timeout: int=TIMEOUT) -> ArchiveResult:
    """
    Download da página completa usando SingleFile,
    com rotação de User-Agent e IP (proxy ZenRows).
    """

    out_dir = out_dir or Path(link.link_dir)
    output = "singlefile.html"

    # Gera User-Agent aleatório
    random_ua = get_random_user_agent()

    # Obtém lista de argumentos padrão do Chrome (1º item costuma ser o binário)
    raw_browser_args = chrome_args(CHROME_TIMEOUT=0)
    # Ignora o primeiro item e remove qualquer "--user-agent=..."
    filtered_browser_args = [
        arg for arg in raw_browser_args[1:]
        if not arg.startswith("--user-agent")
    ]

    # Adicionamos o user-agent e o proxy
    filtered_browser_args.append(f"--user-agent={random_ua}")
    filtered_browser_args.append(f"--proxy-server={ZENROWS_PROXY}")

    # Monta a flag final "--browser-args=..." (JSON no interior)
    # Assim o SingleFile consegue repassar esses args ao Chrome
    browser_args_str = '--browser-args={}'.format(json.dumps(filtered_browser_args))

    # Montamos as opções para o singlefile CLI
    # (a maior parte vem de SINGLEFILE_ARGS, mais o path do Chrome e os browser-args)
    options = [
        *SINGLEFILE_ARGS,
        f'--browser-executable-path={CHROME_BINARY}',
        browser_args_str,
    ]

    # Deduplicate options (pois SingleFile falha se a mesma flag aparece 2x)
    seen_option_names = []
    def test_seen(argument):
        option_name = argument.split("=")[0]
        if option_name in seen_option_names:
            return False
        seen_option_names.append(option_name)
        return True

    deduped_options = list(filter(test_seen, options))

    # Comando final para rodar single-file
    cmd = [
        DEPENDENCIES['SINGLEFILE_BINARY']['path'],
        *deduped_options,
        link.url,      # URL que vamos salvar
        output,        # arquivo de saída (singlefile.html)
    ]

    print('cmd =>', cmd)
    status = 'succeeded'
    timer = TimedProgress(timeout, prefix='      ')

    try:
        # Executamos o comando usando run() do ArchiveBox, que chama subprocess.run().
        result = run(cmd, cwd=str(out_dir), timeout=timeout)
        output_tail = [
            line.strip()
            for line in (result.stdout + result.stderr).decode().rsplit('\n', 3)[-3:]
            if line.strip()
        ]
        hints = (
            f"Got single-file response code: {result.returncode}.",
            *output_tail,
        )

        # Verifica se houve erro (returncode > 0) ou se singlefile.html não foi criado
        if (result.returncode > 0) or not (out_dir / output).is_file():
            raise ArchiveError('SingleFile was not able to archive the page', hints)

        # Ajusta permissões
        chmod_file(output, cwd=str(out_dir))

    except (Exception, OSError) as err:
        status = 'failed'
        output = err
    finally:
        timer.end()

    return ArchiveResult(
        cmd=cmd,
        pwd=str(out_dir),
        cmd_version=SINGLEFILE_VERSION,
        output=output,
        status=status,
        **timer.stats,
    )
