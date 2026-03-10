# Pipeline de Extração — Compras Públicas (dadosabertos.compras.gov.br)

Extrai licitações e contratações públicas via API federal, armazena em cache JSON
e exporta CSVs prontos para consumo no **Power BI**.

---

## Estrutura

```
pipeline_compras/
├── config.py        # ⚙️  Configurações: UASGs, endpoints, anos, parâmetros
├── logger.py        # 📋 Logging centralizado (console + arquivo)
├── cache.py         # 💾 Leitura/escrita de cache JSON
├── api_client.py    # 🌐 Requisições HTTP com backoff exponencial
├── extractors.py    # 🔄 Lógica de extração paginada (Legado + Lei 14.133)
├── exporters.py     # 📤 Consolidação e exportação para CSV
├── main.py          # 🚀 Orquestrador principal
└── pipeline.log     # 📄 Log de execução (gerado automaticamente)
```

---

## Instalação

```bash
pip install requests
```

---

## Execução

```bash
python main.py
```

---

## Saídas

| Arquivo                       | Conteúdo                              |
|-------------------------------|---------------------------------------|
| `output/compras_legado.csv`   | Licitações do módulo Legado           |
| `output/compras_lei14133.csv` | Contratações PNCP (Lei 14.133/2021)   |
| `pipeline.log`                | Log completo da execução              |
| `temp/compras_legado/`   | Cache JSON por UASG/ano/página        |
| `temp/compras_14133/`    | Cache JSON por UASG/modalidade/página |

---

## Configuração rápida (`config.py`)

| Parâmetro                   | O que controla                              |
|-----------------------------|---------------------------------------------|
| `UASGS`                     | Lista de unidades gestoras a extrair        |
| `CONFIG_APIS["LEGADO"]["anos"]`  | Intervalo de anos do módulo Legado     |
| `CONFIG_APIS["LEI14133"]["uasgs"]` | UASGs do módulo PNCP              |
| `PIPELINE_CONFIG["max_workers_legado"]` | Threads paralelas do Legado   |
| `PIPELINE_CONFIG["backoff_tentativas"]` | Retries na API               |
| `EXPORT_CONFIG["separador"]` | Separador do CSV (padrão `;` para pt-BR)    |

---

## Comportamento de cache

- **SKIP**: arquivo já existe com status `SUCESSO` → não consulta a API novamente.
- **Proteção**: falha na API nunca sobrescreve um cache válido anterior.
- **Re-verificação PNCP**: contratos ainda em aberto são re-consultados
  após `dias_validade_cache_pncp` dias (padrão: 7).

---

## Power BI

Os CSVs são gerados com:
- **Encoding**: `utf-8-sig` (BOM) — compatível com Excel e Power BI sem problemas de acentuação.
- **Separador**: `;` — padrão pt-BR.
- **Colunas extras**: `_url_origem`, `_data_extracao`, `_arquivo_origem` para rastreabilidade.
