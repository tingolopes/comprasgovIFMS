"""
api_client.py
-------------
Comunicação com a API dadosabertos.compras.gov.br.
Responsabilidade única: fazer requisições HTTP com retry e backoff.
"""

import time
from typing import Any

import requests

from config import PIPELINE_CONFIG
from logger import log_erro


def consultar_api(url: str, params: dict) -> tuple[dict | None, str]:
    """
    Realiza GET na URL com os parâmetros informados.

    Estratégia de retry: backoff exponencial.
      - Tentativa 1 → aguarda `backoff_inicial` segundos
      - Tentativa 2 → aguarda `backoff_inicial * 2` segundos
      - ...

    Retorna:
        (dados, "SUCESSO") — quando a API responde 200 com campo "resultado"
        (None,  "FALHA")   — após esgotar todas as tentativas
    """
    timeout = PIPELINE_CONFIG["timeout_segundos"]
    tentativas = PIPELINE_CONFIG["backoff_tentativas"]
    atraso = PIPELINE_CONFIG["backoff_inicial"]

    for tentativa in range(1, tentativas + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            if response.status_code == 200:
                dados = response.json()
                if isinstance(dados, dict) and "resultado" in dados:
                    return dados, "SUCESSO"
            else:
                log_erro(
                    "HTTP %d na tentativa %d | URL: %s",
                    response.status_code, tentativa, url,
                )
        except requests.exceptions.Timeout:
            log_erro("Timeout na tentativa %d | URL: %s", tentativa, url)
        except requests.exceptions.ConnectionError as exc:
            log_erro("Conexão falhou na tentativa %d | %s", tentativa, exc)
        except Exception as exc:
            log_erro("Erro inesperado na tentativa %d | %s", tentativa, exc)

        if tentativa < tentativas:
            time.sleep(atraso)
            atraso *= 2  # backoff: 2s → 4s → 8s ...

    return None, "FALHA"
