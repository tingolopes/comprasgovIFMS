"""
explorar_itens.py
-----------------
Script autônomo para consultar TODOS os endpoints de itens disponíveis
para um dado idCompra na API dadosabertos.compras.gov.br.

Endpoints consultados:
  Módulo Legado:
    E2  — 2.1_consultarItemLicitacao_Id           (itens gerais)
    E4  — 4.1_consultarItensPregoes_Id            (itens de pregão + resultados)
    E6  — 6.1_consultarItensComprasSemLicitacao_Id (dispensa/inexigibilidade)

  Módulo Contratações (PNCP / Lei 14.133):
    C6  — 2.1_consultarItensContratacoes_PNCP_14133_Id  (itens)
    C7  — 3.1_consultarResultadoItensContratacoes_Id     (resultado dos itens)

Uso:
    python explorar_itens.py 15813206000472022
    python explorar_itens.py 15813206000472022 --pasta saida/minha_consulta
    python explorar_itens.py 15813206000472022 --sem-salvar
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from urllib.parse import urlencode

import requests

BASE_URL = "https://dadosabertos.compras.gov.br"
TIMEOUT = 30

# ---------------------------------------------------------------------------
# Definição de todos os endpoints de itens disponíveis na API
# ---------------------------------------------------------------------------
ENDPOINTS = [
    {
        "sigla":    "E2",
        "modulo":   "Legado",
        "descricao": "Itens de Licitações (geral)",
        "path":     "/modulo-legado/2.1_consultarItemLicitacao_Id",
        "params_fn": lambda id_c: {"id_compra": id_c},
        "paginavel": False,
    },
    {
        "sigla":    "E4",
        "modulo":   "Legado",
        "descricao": "Itens de Pregões (+ resultado, fornecedor vencedor)",
        "path":     "/modulo-legado/4.1_consultarItensPregoes_Id",
        "params_fn": lambda id_c: {"id_compra": id_c},
        "paginavel": False,
    },
    {
        "sigla":    "E6",
        "modulo":   "Legado",
        "descricao": "Itens de Compras sem Licitação (dispensa/inexigibilidade)",
        "path":     "/modulo-legado/6.1_consultarItensComprasSemLicitacao_Id",
        "params_fn": lambda id_c: {"id_compra": id_c},
        "paginavel": False,
    },
    {
        "sigla":    "C6",
        "modulo":   "PNCP/14133",
        "descricao": "Itens das Contratações PNCP 14133",
        "path":     "/modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id",
        "params_fn": lambda id_c: {"tipo": "idCompra", "codigo": id_c},
        "paginavel": True,
    },
    {
        "sigla":    "C7",
        "modulo":   "PNCP/14133",
        "descricao": "Resultado dos Itens das Contratações PNCP 14133",
        "path":     "/modulo-contratacoes/3.1_consultarResultadoItensContratacoes_Id",
        "params_fn": lambda id_c: {"tipo": "idCompra", "codigo": id_c},
        "paginavel": True,
    },
]


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def _get(url: str, params: dict) -> tuple[dict | None, int | None, str]:
    """
    Retorna (dados, http_status_code, mensagem).
    Tenta até 3 vezes com backoff em caso de 429.
    """
    for tentativa in range(1, 4):
        try:
            resp = requests.get(url, params=params, timeout=TIMEOUT)
            if resp.status_code == 200:
                return resp.json(), 200, "OK"
            elif resp.status_code == 429:
                espera = 15 * tentativa
                print(
                    f"      ⚠️  429 Too Many Requests — aguardando {espera}s...")
                time.sleep(espera)
                continue
            else:
                return None, resp.status_code, f"HTTP {resp.status_code}"
        except Exception as exc:
            print(f"      ⚠️  Tentativa {tentativa} falhou: {exc}")
            time.sleep(3)
    return None, None, "FALHA após 3 tentativas"


# ---------------------------------------------------------------------------
# Consulta de um endpoint (lida com paginação)
# ---------------------------------------------------------------------------

def _consultar_endpoint(ep: dict, id_compra: str) -> dict:
    """
    Consulta um endpoint (com paginação se necessário).
    Retorna um dicionário com todos os resultados e metadados.
    """
    resultado_completo = []
    pagina = 1
    total_paginas = None

    while True:
        params = ep["params_fn"](id_compra)
        if ep["paginavel"]:
            params.update({"pagina": pagina, "tamanhoPagina": 500})

        url = f"{BASE_URL}{ep['path']}"
        dados, http_code, msg = _get(url, params)

        if dados is None:
            return {
                "status":    "FALHA",
                "http_code": http_code,
                "mensagem":  msg,
                "resultado": [],
                "total":     0,
                "paginas":   pagina - 1,
            }

        resultado_pagina = dados.get("resultado", [])
        resultado_completo.extend(
            resultado_pagina if isinstance(resultado_pagina, list) else [])

        if total_paginas is None:
            total_paginas = dados.get("totalPaginas", 1)

        pag_restantes = dados.get("paginasRestantes", 0)

        if not ep["paginavel"] or pag_restantes == 0 or not resultado_pagina:
            break

        pagina += 1
        time.sleep(0.3)   # pausa leve entre páginas

    return {
        "status":    "SUCESSO",
        "http_code": 200,
        "mensagem":  "OK",
        "resultado": resultado_completo,
        "total":     len(resultado_completo),
        "paginas":   pagina,
    }


# ---------------------------------------------------------------------------
# Salvamento
# ---------------------------------------------------------------------------

def _salvar(pasta: str, id_compra: str, sigla: str,
            ep: dict, resultado: dict) -> str:
    """Salva o resultado em JSON e retorna o caminho do arquivo."""
    os.makedirs(pasta, exist_ok=True)
    nome = f"explorar_{id_compra}_{sigla}.json"
    caminho = os.path.join(pasta, nome)

    envelope = {
        "metadata": {
            "id_compra":      id_compra,
            "endpoint_sigla": sigla,
            "endpoint_path":  ep["path"],
            "data_consulta":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status":         resultado["status"],
            "http_code":      resultado["http_code"],
            "total_itens":    resultado["total"],
            "paginas":        resultado["paginas"],
        },
        "resultado": resultado["resultado"],
    }

    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(envelope, f, ensure_ascii=False, indent=2)

    return caminho


# ---------------------------------------------------------------------------
# Exibição resumida dos resultados
# ---------------------------------------------------------------------------

def _resumir(sigla: str, ep: dict, resultado: dict) -> None:
    status = resultado["status"]
    total = resultado["total"]

    if status != "SUCESSO":
        icone = "❌"
        detalhe = resultado["mensagem"]
    elif total == 0:
        icone = "⚪"
        detalhe = "sem registros"
    else:
        icone = "✅"
        detalhe = f"{total} item(ns)"

    print(f"  {icone} {sigla:<3}  [{ep['modulo']:<12}]  {ep['descricao']}")
    print(f"          → {detalhe}")

    # Mostra prévia dos primeiros campos de cada item
    if total > 0:
        for i, item in enumerate(resultado["resultado"][:3], 1):
            # Campos mais informativos dependendo do endpoint
            campos_chave = [
                "idCompraItem", "id_compra_item", "nuItemMaterial",
                "numeroItemCompra", "numeroItemLicitacao",
                "nomeMaterial", "nomeServico", "descricaoItem",
                "descricaoResumida", "noServico", "dsDetalhada",
                "situacaoItem", "dsSituacaoItem", "situacaoItemNome",
                "valorUnitarioEstimado", "valorHomologadoItem",
                "nomeFornecedor", "nomeFornecedorVencedor",
            ]
            resumo = {k: item[k] for k in campos_chave if k in item}
            print(
                f"          item {i}: {json.dumps(resumo, ensure_ascii=False)[:200]}")
        if total > 3:
            print(f"          ... e mais {total - 3} item(ns)")


# ---------------------------------------------------------------------------
# Principal
# ---------------------------------------------------------------------------

def explorar(id_compra: str, pasta: str | None, salvar: bool) -> None:
    print()
    print("=" * 65)
    print(f"  EXPLORADOR DE ITENS — idCompra: {id_compra}")
    print(f"  Data: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    if salvar and pasta:
        print(f"  Pasta de saída: {pasta}")
    print("=" * 65)
    print()

    resumo_total = 0

    for ep in ENDPOINTS:
        sigla = ep["sigla"]
        print(f"  🔍 Consultando {sigla} — {ep['descricao']}...")
        resultado = _consultar_endpoint(ep, id_compra)

        _resumir(sigla, ep, resultado)

        if salvar and pasta and resultado["status"] == "SUCESSO":
            caminho = _salvar(pasta, id_compra, sigla, ep, resultado)
            print(f"          💾 Salvo: {caminho}")

        resumo_total += resultado["total"]
        print()

    print("=" * 65)
    print(f"  Total de itens encontrados (todos os endpoints): {resumo_total}")
    print("=" * 65)
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args():
    parser = argparse.ArgumentParser(
        description="Consulta todos os endpoints de itens para um idCompra.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "id_compra",
        help="O idCompra a consultar. Ex: 15813206000472022",
    )
    parser.add_argument(
        "--pasta",
        default="temp/explorar_saida",
        help="Pasta onde salvar os JSONs (padrão: explorar_saida/)",
    )
    parser.add_argument(
        "--sem-salvar",
        action="store_true",
        help="Apenas exibe os resultados, sem salvar arquivos.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    explorar(
        id_compra=args.id_compra,
        pasta=None if args.sem_salvar else args.pasta,
        salvar=not args.sem_salvar,
    )
