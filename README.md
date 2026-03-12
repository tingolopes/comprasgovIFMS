# ComprasGov IFMS - Pipeline de Dados de Compras Públicas

Um pipeline de extração, transformação e consolidação de dados de compras públicas do Instituto Federal de Mato Grosso do Sul (IFMS).

## 📋 Descrição

Este projeto extrai dados de compras públicas do IFMS a partir da API pública de dados abertos do Governo Federal (`dadosabertos.compras.gov.br`). O pipeline coleta informações sobre:

- **Compras Legadas** (módulo legado até 2021): licitações, pregões, dispensas
- **Compras Lei 14.133** (novo marco legal a partir de 2021)
- **Itens de Compras**: produtos/serviços associados a cada compra
- **Atas de Registro de Preço (ARP)**: atas de compra centralizada

Os dados são consolidados em arquivos CSV para análise e consulta.

## 📁 Estrutura do Projeto

```
comprasgovIFMS/
├── main.py                          # Orquestrador principal do pipeline
├── config/
│   └── config.py                    # Configurações centralizadas (UASGs, APIs, endpoints)
├── pipeline/
│   ├── api_client.py                # Cliente HTTP e gerenciamento de cache
│   ├── extractors_compras.py        # Extração de dados do módulo legado e Lei 14.133
│   ├── extractors_itens.py          # Extração de itens de compras
│   ├── extractors_atas.py           # Extração de atas de registro de preço
│   ├── transformer_compras.py       # Consolidação de compras em CSV
│   ├── transformer_itens.py         # Consolidação de itens em CSV
│   ├── transformer_atas.py          # Consolidação de atas em CSV
│   └── logger.py                    # Logging e resumo de execução
├── data/
│   ├── compras.csv                  # dataset consolidado de compras
│   ├── itens.csv                    # dataset consolidado de itens
│   └── atas.csv                     # dataset consolidado de atas
├── temp/
│   ├── compras/                     # Cache JSON das compras por período/modalidade
│   ├── itens/                       # Cache JSON dos itens por compra
│   ├── atas/                        # Cache JSON das atas por período
│   └── *.py                         # Scripts auxiliares de análise/diagnóstico
└── README.md
```

## 🚀 Como Usar

### Pré-requisitos

- Python 3.8+
- Pacotes necessários (instalados com `pip`):
  - `requests` — requisições HTTP
  - `pandas` — processamento de dados

### Instalação

```bash
# 1. Clonar ou baixar o projeto
cd comprasgovIFMS

# 2. Instalar dependências
pip install requests pandas
```

### Execução

#### Pipeline Completo

Executa toda a sequência: extração de compras, itens e atas, seguida de consolidação em CSV.

```bash
python main.py
```

**Fluxo:**
1. Extrai compras — módulo Legado → `temp/compras/`
2. Extrai compras — módulo Lei 14.133 → `temp/compras/`
3. Consolida JSONs → `data/compras.csv`
4. Extrai itens de cada compra → `temp/itens/`
5. Consolida itens → `data/itens.csv`
6. Extrai atas de registro de preço → `temp/atas/`
7. Consolida atas → `data/atas.csv`

#### Modos Específicos

Execute apenas uma etapa do pipeline:

```bash
# Consolidar compras (sem re-extrair)
python main.py --modo transformer_compras

# Extrair e consolidar itens
python main.py --modo extrator_itens

# Consolidar itens (sem re-extrair)
python main.py --modo transformer_itens

# Extrair e consolidar atas
python main.py --modo extrator_atas

# Consolidar atas (sem re-extrair)
python main.py --modo transformer_atas
```

## ⚙️ Configuração

Todas as configurações ficam centralizadas em [config/config.py](config/config.py):

### Unidades Gestoras (UASGs)

O projeto cobre 11 unidades do IFMS:

| Sigla | Código   | Nome |
|-------|----------|------|
| RT    | 158132   | IFMS REITORIA |
| AQ    | 158448   | IFMS CAMPUS AQUIDAUANA |
| CG    | 158449   | IFMS CAMPUS CAMPO GRANDE |
| CB    | 158450   | IFMS CAMPUS CORUMBA |
| CX    | 158451   | IFMS CAMPUS COXIM |
| DR    | 155848   | IFMS CAMPUS DOURADOS |
| JD    | 155850   | IFMS CAMPUS JARDIM |
| NA    | 158452   | IFMS CAMPUS NOVA ANDRADINA |
| NV    | 155849   | IFMS CAMPUS NAVIRAÍ |
| PP    | 158453   | IFMS CAMPUS PONTA PORÃ |
| TL    | 158454   | IFMS CAMPUS TRÊS LAGOAS |

Para modificar a lista de unidades, edite `UASGS` em [config/config.py](config/config.py).

### Intervalo de Anos

Por padrão, o pipeline coleta dados de:
- **Módulo Legado:** 2016 até o ano atual
- **Lei 14.133:** 2021 até o ano atual

Ajuste em `CONFIG_APIS['LEGADO']['anos']` ou `CONFIG_APIS['LEI14133']['anos']`.

### Endpoints

Os endpoints consultados são configuráveis em `CONFIG_APIS`. Atualmente:

**Módulo Legado:**
- Outras Modalidades (licitações abertas)
- Pregões
- Dispensas

**Lei 14.133 (PNCP):**
- Contratações por modalidade

**Atas de Registro de Preço:**
- Consolidado nacional (filtrando por UASG)

## 📊 Dados Utilizados

### Compras

Dados obtidos da API `/modulo-legado/` e `/modulo-contratacoes/` do portal de dados abertos.

**Campos principais:**
- ID da compra, modalidade, status
- Objeto, valor, data de publicação
- Unidade gestora
- Empresa contratada

### Itens

Cada compra pode ter múltiplos itens (produtos/serviços).

**Campos principais:**
- Item ID, descrição
- Quantidade, valor unitário
- Categoria (CATMAT/CATSER)

### Atas de Registro de Preço

Dados de atas de compra centralizada (Lei 14.133).

**Campos principais:**
- ARP ID, número da ata
- Data de vigência
- Fornecedores, empresas
- Preços registrados

## 🔄 Cache e Performance

O pipeline utiliza **cache em disco** para evitar re-consultas desnecessárias:

- Arquivos JSON são armazenados em `temp/compras/`, `temp/itens/`, `temp/atas/`
- Cache é validado por status (marcado como `SUCESSO` ou `FALHA`)
- Dados com sucesso não são sobrescritos por falhas

Para limpar o cache e re-extrair tudo, delete os arquivos JSON de `temp/`.

## 📈 Scripts Auxiliares

Em `temp/` há vários scripts de análise:

| Script | Descrição |
|--------|-----------|
| `analisar_csv.py` | Analisa estatísticas dos CSVs gerados |
| `analisar_cobertura_itens.py` | Verifica cobertura de itens por compra |
| `diagnosticar_id.py` | Diagnóstico de IDs duplicados/faltantes |
| `explorar_itens.py` | Exploração de estrutura de itens |
| `limpar_itens.py` | Limpeza e validação de dados de itens |
| `recuperar_cache.py` | Recuperação de dados de cache corrompidos |

Exemplo:

```bash
python temp/analisar_csv.py
```

## 📝 Logging

O pipeline registra:

- **DONE** — Requisição bem-sucedida
- **FAIL** — Erro na requisição
- **SKIP** — Dado já em cache (não consultado novamente)

Um resumo é exibido ao final da execução com estatísticas de sucesso/falha.

## 🐛 Troubleshooting

### "Erro de conexão com a API"
- Verifique a conectividade com `https://dadosabertos.compras.gov.br`
- A API pode estar indisponível temporariamente

### "Arquivo de cache inválido"
- Delete o arquivo JSON problemático em `temp/`
- Re-execute o pipeline para regenerar

### "CSV vazio ou incompleto"
- Verifique se há dados em `temp/compras/`, `temp/itens/` ou `temp/atas/`
- Use scripts de análise: `python temp/analisar_csv.py`

## 📞 Suporte

Para dúvidas sobre a API de dados abertos, consulte:
- Portal: https://dadosabertos.compras.gov.br
- Documentação API: https://dadosabertos.compras.gov.br/docs

## 📄 Licença

Este projeto está associado ao Instituto Federal de Mato Grosso do Sul (IFMS).
