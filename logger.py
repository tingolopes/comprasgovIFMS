"""
logger.py
---------
Logging centralizado com controle de flood para mensagens de SKIP.
Todas as outras partes do pipeline importam o `log` daqui.
"""

import logging
from datetime import datetime
from threading import Lock

from config import PIPELINE_CONFIG


# ---------------------------------------------------------------------------
# Logger padrão Python (arquivo + console)
# ---------------------------------------------------------------------------
def _criar_logger() -> logging.Logger:
    logger = logging.getLogger("pipeline_compras")
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
    )

    # Console
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # Arquivo (debug completo)
    fh = logging.FileHandler("pipeline.log", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger


logger = _criar_logger()


# ---------------------------------------------------------------------------
# Controle de flood para SKIPs
# ---------------------------------------------------------------------------
class _SkipCounter:
    """Imprime um resumo de SKIPs a cada N ocorrências para não poluir o log."""

    def __init__(self, intervalo: int):
        self._intervalo = intervalo
        self._total = 0
        self._ultimo_log = 0
        self._lock = Lock()

    def registrar(self) -> None:
        with self._lock:
            self._total += 1
            pendentes = self._total - self._ultimo_log
            if pendentes >= self._intervalo:
                self._ultimo_log = self._total
                logger.info("⏭️  SKIPs acumulados: %d", self._total)

    @property
    def total(self) -> int:
        return self._total


_skip_counter = _SkipCounter(PIPELINE_CONFIG["log_intervalo_skip"])


# ---------------------------------------------------------------------------
# Interface pública
# ---------------------------------------------------------------------------
def log_skip(sigla: str, label: str, ano: int) -> None:
    logger.debug("SKIP | %s | %-15s | %d", sigla, label.upper(), ano)
    _skip_counter.registrar()


def log_sucesso(sigla: str, label: str, ano: int, pagina: int) -> None:
    logger.info("✅ DONE | %s | %-15s | %d | p%d",
                sigla, label.upper(), ano, pagina)


def log_falha(sigla: str, label: str, ano: int, pagina: int) -> None:
    logger.warning("❌ FAIL | %s | %-15s | %d | p%d",
                   sigla, label.upper(), ano, pagina)


def log_info(msg: str, *args) -> None:
    logger.info(msg, *args)


def log_erro(msg: str, *args) -> None:
    logger.error(msg, *args)


def resumo_skips() -> int:
    return _skip_counter.total
