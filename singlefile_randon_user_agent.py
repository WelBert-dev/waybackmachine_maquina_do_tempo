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

# Função para gerar um User-Agent aleatório
def get_random_user_agent():
    chrome_versions = [f"{major}.{minor}.0.0" for major in range(100, 125) for minor in range(0, 5)]
    firefox_versions = [f"{major}.0" for major in range(90, 120)]
    safari_versions = [f"{major}.{minor}" for major in range(14, 17) for minor in range(0, 6)]
    edge_versions = [f"{major}.{minor}.{patch}.{build}" for major in range(100, 125)
                     for minor in range(0, 5) for patch in range(1000, 1100) for build in range(50, 70)]

    platforms = {
        "Windows": [
            "Windows NT 10.0; Win64; x64",
            "Windows NT 10.0; Win64",
            "Windows NT 10.0"
        ],
        "Mac": [
            "Macintosh; Intel Mac OS X 10_15_7",
            "Macintosh; Intel Mac OS X 11_6_5",
            "Macintosh; Intel Mac OS X 12_4",
            "Macintosh; Intel Mac OS X 13_3"
        ],
        "Linux": [
            "X11; Linux x86_64",
            "X11; Ubuntu; Linux x86_64",
            "X11; Fedora; Linux x86_64"
        ],
        "Android": [
            "Linux; Android 13; SM-G998B",
            "Linux; Android 12; Pixel 6 Pro",
            "Linux; Android 11; OnePlus 8T",
            "Linux; Android 10; Mi 10"
        ],
        "iPhone": [
            "iPhone; CPU iPhone OS 16_6_1 like Mac OS X",
            "iPhone; CPU iPhone OS 15_4_1 like Mac OS X"
        ]
    }

    browser_choice = random.choice(["Chrome", "Firefox", "Safari", "Edge", "Mobile"])

    if browser_choice == "Chrome":
        platform = random.choice(platforms["Windows"] + platforms["Mac"] + platforms["Linux"])
        version = random.choice(chrome_versions)
        return f"Mozilla/5.0 ({platform}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Safari/537.36"

    elif browser_choice == "Firefox":
        platform = random.choice(platforms["Windows"] + platforms["Mac"] + platforms["Linux"])
        version = random.choice(firefox_versions)
        return f"Mozilla/5.0 ({platform}; rv:{version}) Gecko/20100101 Firefox/{version}"

    elif browser_choice == "Safari":
        platform = random.choice(platforms["Mac"])
        version = random.choice(safari_versions)
        return f"Mozilla/5.0 ({platform}) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/{version} Safari/605.1.15"

    elif browser_choice == "Edge":
        platform = random.choice(platforms["Windows"])
        version = random.choice(edge_versions)
        return f"Mozilla/5.0 ({platform}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version.split('.')[0]}.0.0.0 Safari/537.36 Edg/{version}"

    elif browser_choice == "Mobile":
        platform = random.choice(platforms["Android"] + platforms["iPhone"])
        version = random.choice(chrome_versions)
        return f"Mozilla/5.0 ({platform}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Mobile Safari/537.36"

@enforce_types
def should_save_singlefile(link: Link, out_dir: Optional[Path] = None, overwrite: Optional[bool] = False) -> bool:
    if is_static_file(link.url):
        return False

    out_dir = out_dir or Path(link.link_dir)
    if not overwrite and (out_dir / 'singlefile.html').exists():
        return False

    return SAVE_SINGLEFILE

@enforce_types
def save_singlefile(link: Link, out_dir: Optional[Path] = None, timeout: int = TIMEOUT) -> ArchiveResult:
    """Download full site using SingleFile."""
    out_dir = out_dir or Path(link.link_dir)
    output = "singlefile.html"

    # Obter os argumentos padrão para o Chrome
    browser_args = chrome_args(CHROME_TIMEOUT=0)
    # Montar a string JSON para os argumentos do navegador
    browser_args = '--browser-args={}'.format(json.dumps(browser_args[1:]))
    
    # Gera o user-agent aleatório uma única vez
    random_ua = get_random_user_agent()

    # Obtém os argumentos padrão para o Chrome (exceto o primeiro) e remove qualquer opção de user-agent
    raw_browser_args = chrome_args(CHROME_TIMEOUT=0)[1:]
    filtered_browser_args = [arg for arg in raw_browser_args if not arg.startswith("--user-agent")]
    # Adiciona o user-agent gerado (único)
    filtered_browser_args.append(f"--user-agent={random_ua}")
    # Constrói a string JSON para o parâmetro --browser-args
    browser_args = '--browser-args={}'.format(json.dumps(filtered_browser_args))

    # Constrói as opções a partir de SINGLEFILE_ARGS, adiciona o caminho do Chrome e os argumentos do navegador
    options = [
        *SINGLEFILE_ARGS,
        '--browser-executable-path={}'.format(CHROME_BINARY),
        browser_args,
    ]
    # Remove qualquer opção que defina user-agent (caso haja em SINGLEFILE_ARGS)
    options = [opt for opt in options if not opt.startswith("--user-agent")]

    # Define os parâmetros adicionais desejados, reutilizando o mesmo user-agent gerado
    additional_params = [
        f"--browser-timeout={timeout}",
        f"--max-wait-time={timeout}",
        "--load-deferred-images=false",
        "--load-lazy-loaded-images=false",
        "--compress-HTML=true",
        f"--user-agent={random_ua}"
    ]

    # Deduplicação de opções (mantendo a ordem)
    seen_option_names = []
    def test_seen(argument):
        option_name = argument.split("=")[0]
        if option_name in seen_option_names:
            return False
        else:
            seen_option_names.append(option_name)
            return True
    deduped_options = list(filter(test_seen, options))

    # Constrói o comando final, inserindo os parâmetros adicionais e as opções (já com o browser_args atualizado)
    cmd = [
        DEPENDENCIES['SINGLEFILE_BINARY']['path'],
        *additional_params,
        *deduped_options,
        link.url,
        output,
    ]
    print('cmd :>>', cmd)

    status = 'succeeded'
    timer = TimedProgress(timeout, prefix='      ')
    try:
        result = run(cmd, cwd=str(out_dir), timeout=timeout)
        print('result :>>', result)

        # Extrair as últimas linhas de stderr para identificar detalhes da execução
        output_tail = [
            line.strip()
            for line in (result.stdout + result.stderr).decode().rsplit('\n', 3)[-3:]
            if line.strip()
        ]
        print('output_tail :>>', output_tail)
        hints = (
            'Got single-file response code: {}.'.format(result.returncode),
            *output_tail,
        )
        print('hints :>>', hints)

        if (result.returncode > 0) or not (out_dir / output).is_file():
            raise ArchiveError('SingleFile was not able to archive the page', hints)
        chmod_file(output, cwd=str(out_dir))
    except (Exception, OSError) as err:
        status = 'failed'
        # Ajusta a string para escapar as aspas internas se necessário
        cmd[2] = browser_args.replace('"', "\\\"")
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
