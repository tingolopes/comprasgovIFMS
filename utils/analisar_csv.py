"""
analisar_csv.py
---------------
Análise exploratória dos CSVs gerados pelo pipeline (compras.csv e compras_itens.csv).
Mostra qualidade dos dados, colunas vazias, distribuições e inconsistências.

Uso:
    python analisar_csv.py                          # analisa ambos
    python analisar_csv.py --arquivo data/compras.csv
    python analisar_csv.py --arquivo data/compras_itens.csv
    python analisar_csv.py --exportar relatorio.txt
"""

import argparse
import csv
import os
import re
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

ARQUIVOS_PADRAO = [
    "data/compras.csv",
    "data/compras_itens.csv",
]

# Colunas de valor (para análise numérica)
COLUNAS_VALOR = {
    "compras.csv": ["valor_estimado", "valor_homologado", "valor_contratado"],
    "compras_itens.csv":   ["valor_estimado_item", "valor_unitario_estimado",
                            "valor_unitario_resultado", "valor_total_resultado", "quantidade"],
}

# Colunas de data
COLUNAS_DATA = {
    "compras.csv": ["data_publicacao", "data_abertura", "data_homologacao", "data_extracao"],
    "compras_itens.csv":   ["data_alteracao", "data_inclusao_pncp", "data_resultado", "data_extracao"],
}


# ---------------------------------------------------------------------------
# Utilitários
# ---------------------------------------------------------------------------

def _ler_csv(caminho: str) -> tuple[list[str], list[dict]]:
    with open(caminho, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        colunas = reader.fieldnames or []
        linhas = list(reader)
    return list(colunas), linhas


def _vazio(valor: Any) -> bool:
    return valor is None or str(valor).strip() in ("", "null", "None", "nan")


def _parse_numero(valor: str) -> float | None:
    try:
        return float(str(valor).replace(",", ".").replace(" ", ""))
    except (ValueError, TypeError):
        return None


def _sep(char="─", largura=65) -> str:
    return char * largura


def _titulo(texto: str) -> str:
    return f"\n{'='*65}\n  {texto}\n{'='*65}\n"


def _subtitulo(texto: str) -> str:
    return f"\n  {_sep('─', 61)}\n  {texto}\n  {_sep('─', 61)}\n"


# ---------------------------------------------------------------------------
# Análises
# ---------------------------------------------------------------------------

def analisar_preenchimento(colunas: list[str], linhas: list[dict]) -> list[dict]:
    """Retorna % de preenchimento por coluna, ordenado do mais vazio ao mais cheio."""
    total = len(linhas)
    resultado = []
    for col in colunas:
        vazios = sum(1 for l in linhas if _vazio(l.get(col)))
        pct_vazio = vazios / total * 100 if total else 0
        resultado.append({
            "coluna":    col,
            "preench":   total - vazios,
            "vazios":    vazios,
            "pct_vazio": pct_vazio,
        })
    return sorted(resultado, key=lambda x: -x["pct_vazio"])


def analisar_numerica(col: str, linhas: list[dict]) -> dict:
    valores = [_parse_numero(l.get(col, "")) for l in linhas]
    valores = [v for v in valores if v is not None and v > 0]
    if not valores:
        return {}
    valores.sort()
    n = len(valores)
    total = sum(valores)
    return {
        "count":    n,
        "soma":     total,
        "media":    total / n,
        "mediana":  valores[n // 2],
        "min":      valores[0],
        "max":      valores[-1],
        "zeros":    sum(1 for v in valores if v == 0),
    }


def analisar_categorica(col: str, linhas: list[dict], top: int = 10) -> list[tuple]:
    contador = Counter(
        str(l.get(col, "")).strip()
        for l in linhas
        if not _vazio(l.get(col))
    )
    return contador.most_common(top)


def _parse_data(valor: str) -> datetime | None:
    """Tenta parsear uma data em múltiplos formatos. Retorna None se inválida."""
    v = str(valor).strip()
    # (formato, tamanho esperado da string de data)
    tentativas = [
        ("%Y-%m-%d %H:%M:%S", 19),
        ("%Y-%m-%dT%H:%M:%S", 19),
        ("%Y-%m-%d",          10),
        ("%d/%m/%Y",          10),
    ]
    for fmt, tam in tentativas:
        try:
            return datetime.strptime(v[:tam], fmt)
        except ValueError:
            continue
    return None


def detectar_inconsistencias(colunas: list[str], linhas: list[dict],
                             nome_arquivo: str) -> list[str]:
    problemas = []
    total = len(linhas)
    base = os.path.basename(nome_arquivo)

    # IDs duplicados
    col_id = next((c for c in colunas if "id_compra_item" in c), None) or \
        next((c for c in colunas if c == "id_compra"), None)
    if col_id:
        ids = [l.get(col_id, "") for l in linhas]
        dupes = total - len(set(ids))
        if dupes:
            problemas.append(
                f"⚠️  {dupes} IDs duplicados na coluna '{col_id}'")

    # Valores negativos e outliers
    for col in COLUNAS_VALOR.get(base, []):
        if col not in colunas:
            continue
        valores = [_parse_numero(l.get(col)) for l in linhas]
        valores_validos = [v for v in valores if v is not None]

        negativos = sum(1 for v in valores_validos if v < 0)
        if negativos:
            problemas.append(f"⚠️  {negativos} valores negativos em '{col}'")

        # Outliers: valores acima de 100× a mediana (apenas se mediana > 0)
        if len(valores_validos) > 10:
            sv = sorted(v for v in valores_validos if v > 0)
            if sv:
                mediana = sv[len(sv) // 2]
                if mediana > 0:
                    limite = mediana * 100
                    outliers = [(v, i) for i, v in enumerate(
                        valores_validos) if v > limite]
                    if outliers:
                        problemas.append(
                            f"⚠️  {len(outliers)} outlier(s) em '{col}' "
                            f"(>{limite:,.0f} | mediana={mediana:,.2f}) "
                            f"— ex: R$ {max(v for v,_ in outliers):,.2f}"
                        )

    # Datas inválidas / futuras — conta por DATA, não por tentativa de formato
    hoje = datetime.now()
    for col in COLUNAS_DATA.get(base, []):
        if col not in colunas:
            continue
        futuras = invalidas = 0
        for l in linhas:
            v = str(l.get(col, "")).strip()
            if not v or _vazio(v):
                continue
            dt = _parse_data(v)
            if dt is None:
                invalidas += 1
            elif dt > hoje:
                futuras += 1

        if invalidas:
            problemas.append(f"⚠️  {invalidas} datas inválidas em '{col}'")
        if futuras:
            problemas.append(f"⚠️  {futuras} datas futuras em '{col}'")

    if not problemas:
        problemas.append("✅ Nenhuma inconsistência detectada")

    return problemas


# ---------------------------------------------------------------------------
# Relatório
# ---------------------------------------------------------------------------

def relatorio(caminho: str, saida: list[str]) -> None:
    def p(texto=""):
        saida.append(texto)
        print(texto)

    nome = os.path.basename(caminho)
    p(_titulo(f"ANÁLISE — {nome.upper()}"))

    if not os.path.exists(caminho):
        p(f"  ❌ Arquivo não encontrado: {caminho}")
        return

    colunas, linhas = _ler_csv(caminho)
    total = len(linhas)
    tam = os.path.getsize(caminho) / 1024

    p(f"  Arquivo    : {caminho}")
    p(f"  Tamanho    : {tam:.1f} KB")
    p(f"  Linhas     : {total:,}")
    p(f"  Colunas    : {len(colunas)}")

    # ── Preenchimento ────────────────────────────────────────────────────
    p(_subtitulo("PREENCHIMENTO DAS COLUNAS"))
    preench = analisar_preenchimento(colunas, linhas)

    completamente_vazias = [r for r in preench if r["pct_vazio"] == 100]
    muito_vazias = [r for r in preench if 50 <= r["pct_vazio"] < 100]
    parcialmente_vazias = [r for r in preench if 0 < r["pct_vazio"] < 50]
    completas = [r for r in preench if r["pct_vazio"] == 0]

    p(f"  ✅ Completas (100% preenchidas)  : {len(completas)}")
    p(f"  🟡 Parcialmente vazias (<50%)    : {len(parcialmente_vazias)}")
    p(f"  🟠 Muito vazias (≥50%)           : {len(muito_vazias)}")
    p(f"  ❌ Completamente vazias (100%)   : {len(completamente_vazias)}")

    if completamente_vazias:
        p(f"\n  Colunas 100% vazias:")
        for r in completamente_vazias:
            p(f"    ❌  {r['coluna']}")

    if muito_vazias:
        p(f"\n  Colunas muito vazias (≥50% vazios):")
        for r in muito_vazias:
            p(f"    🟠  {r['coluna']:<40} {r['pct_vazio']:>5.1f}% vazio  ({r['preench']:,} preenchidos)")

    if parcialmente_vazias:
        p(f"\n  Colunas parcialmente vazias:")
        for r in parcialmente_vazias:
            p(f"    🟡  {r['coluna']:<40} {r['pct_vazio']:>5.1f}% vazio  ({r['preench']:,} preenchidos)")

    # ── Análise numérica ─────────────────────────────────────────────────
    base = os.path.basename(caminho)
    cols_valor = [c for c in COLUNAS_VALOR.get(base, []) if c in colunas]
    if cols_valor:
        p(_subtitulo("ANÁLISE DE VALORES NUMÉRICOS"))
        for col in cols_valor:
            stats = analisar_numerica(col, linhas)
            if not stats:
                p(f"  {col}: sem dados numéricos válidos")
                continue
            p(f"  {col}:")
            p(f"    Registros c/ valor : {stats['count']:,}")
            p(f"    Soma total         : R$ {stats['soma']:,.2f}")
            p(f"    Média              : R$ {stats['media']:,.2f}")
            p(f"    Mediana            : R$ {stats['mediana']:,.2f}")
            p(f"    Mínimo             : R$ {stats['min']:,.2f}")
            p(f"    Máximo             : R$ {stats['max']:,.2f}")

    # ── Distribuições categóricas ─────────────────────────────────────────
    p(_subtitulo("DISTRIBUIÇÕES CATEGÓRICAS"))

    cols_cat = []
    if "compras" in base:
        cols_cat = ["modalidade", "situacao",
                    "uasg", "ano", "lei_14133", "modulo"]
    elif "itens" in base:
        cols_cat = ["modulo", "tipo_material_servico", "situacao_item",
                    "nome_fornecedor", "unidade_medida"]

    for col in cols_cat:
        if col not in colunas:
            continue
        top = analisar_categorica(col, linhas)
        if not top:
            continue
        unicos = len(set(str(l.get(col, ""))
                     for l in linhas if not _vazio(l.get(col))))
        p(f"\n  {col}  ({unicos} valores únicos — top 10):")
        for valor, cnt in top:
            pct = cnt / total * 100
            barra = "█" * int(pct / 2)
            p(f"    {str(valor)[:45]:<45} {cnt:>6,}  {pct:>5.1f}%  {barra}")

    # ── Inconsistências ───────────────────────────────────────────────────
    p(_subtitulo("VERIFICAÇÃO DE INCONSISTÊNCIAS"))
    for msg in detectar_inconsistencias(colunas, linhas, caminho):
        p(f"  {msg}")

    # ── Cobertura por ano ─────────────────────────────────────────────────
    col_ano = next((c for c in colunas if c == "ano"), None)
    if col_ano:
        p(_subtitulo("COBERTURA POR ANO"))
        por_ano = Counter(str(l.get(col_ano, "")).strip() for l in linhas
                          if not _vazio(l.get(col_ano)))
        for ano, cnt in sorted(por_ano.items()):
            pct = cnt / total * 100
            barra = "█" * int(pct / 3)
            p(f"    {ano}  {cnt:>6,}  {pct:>5.1f}%  {barra}")

    # ── Cobertura por UASG ────────────────────────────────────────────────
    col_uasg = next(
        (c for c in colunas if "uasg" in c and "nome" not in c), None)
    if col_uasg:
        p(_subtitulo("COBERTURA POR UASG"))
        por_uasg = Counter(str(l.get(col_uasg, "")).strip() for l in linhas
                           if not _vazio(l.get(col_uasg)))
        # Tenta pegar nome da uasg se disponível
        col_nome_uasg = next(
            (c for c in colunas if "nome_uasg" in c or "nome_unidade" in c), None)
        nomes: dict[str, str] = {}
        if col_nome_uasg:
            for l in linhas:
                u = str(l.get(col_uasg, "")).strip()
                n = str(l.get(col_nome_uasg, "")).strip()
                if u and n and u not in nomes:
                    nomes[u] = n

        for uasg, cnt in sorted(por_uasg.items(), key=lambda x: -x[1]):
            nome = nomes.get(uasg, "")[:30]
            pct = cnt / total * 100
            p(f"    {uasg}  {nome:<30}  {cnt:>6,}  {pct:>5.1f}%")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Análise exploratória dos CSVs do pipeline de compras."
    )
    parser.add_argument("--arquivo",   default=None,
                        help="Caminho de um CSV específico. Se omitido, analisa ambos.")
    parser.add_argument("--exportar",  default=None,
                        help="Salva o relatório em arquivo texto (ex: relatorio.txt)")
    args = parser.parse_args()

    arquivos = [args.arquivo] if args.arquivo else ARQUIVOS_PADRAO
    saida: list[str] = []

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cabecalho = f"\n{'#'*65}\n  RELATÓRIO DE QUALIDADE DE DADOS — {ts}\n{'#'*65}"
    print(cabecalho)
    saida.append(cabecalho)

    for arq in arquivos:
        relatorio(arq, saida)

    if args.exportar:
        caminho_relatorio = os.path.join(os.path.dirname(
            os.path.abspath(__file__)), args.exportar)
        with open(caminho_relatorio, "w", encoding="utf-8") as f:
            f.write("\n".join(saida))
        print(f"\n💾 Relatório salvo em: {caminho_relatorio}")


if __name__ == "__main__":
    main()
