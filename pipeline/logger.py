"""
pipeline/logger.py
------------------
Logging centralizado para o pipeline.
Usa logging padrão do Python com formato consistente.
"""

import logging
from threading import Lock

_lock = Lock()
_skip_count = 0

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)

_logger = logging.getLogger("pipeline")


def log_info(msg: str, *args) -> None:
    _logger.info(msg, *args)


def log_aviso(msg: str, *args) -> None:
    _logger.warning(msg, *args)


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
