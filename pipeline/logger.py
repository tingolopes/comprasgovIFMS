"""
pipeline/logger.py
------------------
Logging centralizado para o pipeline.
Usa logging padrão do Python com formato consistente.
"""

import logging
import sys
import os
from threading import Lock
from datetime import datetime

_lock = Lock()
_skip_count = 0

class LoggerWriter:
    """Redireciona stdout/stderr para o logger para capturar 'prints' de outros módulos."""
    def __init__(self, level):
        self.level = level

    def write(self, message):
        if message.strip():
            self.level(message.strip())

    def flush(self):
        pass

def configurar_logging() -> str:
    """Configura o logging para gravar em arquivo (data/log/) e console simultaneamente."""
    # Cria diretório de logs se não existir
    os.makedirs("data/log", exist_ok=True)
    
    # Gera nome do arquivo com data e hora
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    arquivo = f"data/log/pipeline_{timestamp}.log"

    # Remove handlers existentes
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # Configuração básica
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.FileHandler(arquivo, encoding="utf-8"),
            logging.StreamHandler(sys.stdout)
        ]
    )

    # Captura prints e erros diretos de outros módulos que não usam log_info
    sys.stdout = LoggerWriter(logging.getLogger("STDOUT").info)
    sys.stderr = LoggerWriter(logging.getLogger("STDERR").error)

    return arquivo

# Configuração padrão mínima
if not logging.root.handlers:
    logging.basicConfig(
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
        level=logging.INFO,
    )

_logger = logging.getLogger("pipeline")


def log_info(msg: str, *args) -> None:
    _logger.info(msg, *args)


def log_erro(msg: str, *args) -> None:
    _logger.error(msg, *args)


def log_skip() -> None:
    """Incrementa o contador de skips (thread-safe)."""
    global _skip_count
    with _lock:
        _skip_count += 1


def resumo_skips() -> int:
    return _skip_count


def resetar_skips() -> None:
    global _skip_count
    with _lock:
        _skip_count = 0
