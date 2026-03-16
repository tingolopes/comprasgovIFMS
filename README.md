# ComprasGov IFMS - Pipeline de Dados de Compras Públicas

Um pipeline de extração, transformação e consolidação de dados de compras públicas do Instituto Federal de Mato Grosso do Sul (IFMS).

## 📋 Descrição

Este projeto extrai dados de compras públicas do IFMS a partir da API pública de dados abertos do Governo Federal (`dadosabertos.compras.gov.br`). O pipeline coleta informações sobre:

- **Compras Legadas** (módulo legado até 2021): licitações, pregões, dispensas
- **Compras Lei 14.133** (novo marco legal a partir de 2021)
- **Itens de Compras**: produtos/serviços associados a cada compra
- **Atas de Registro de Preço (ARP)**: atas de compra centralizada
- **Itens das Atas**: produtos/serviços registrados nas atas
- **Saldos das Atas**: saldos remanescentes dos itens nas atas
- **Unidades Participantes**: unidades gestoras participantes das atas

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
│   ├── extractors_compras_itens.py   # Extração de itens de compras
│   ├── extractors_atas.py           # Extração de atas de registro de preço
│   ├── extractors_atas_itens.py     # Extração de itens das atas
│   ├── extractors_atas_saldos.py    # Extração de saldos das atas
│   ├── extractors_atas_unidades.py  # Extração de unidades participantes das atas
│   ├── transformer_compras.py       # Consolidação de compras em CSV
│   ├── transformer_compras_itens.py # Consolidação de itens em CSV
│   ├── transformer_atas.py          # Consolidação de atas em CSV
│   ├── transformer_atas_itens.py    # Consolidação de itens das atas em CSV
│   ├── transformer_atas_saldos.py   # Consolidação de saldos em CSV
│   ├── transformer_atas_unidades.py # Consolidação de unidades em CSV
│   ├── extractors_contratos.py      # Extração de contratos (contratos.comprasnet.gov.br)
│   ├── transformer_contratos.py     # Consolidação de contratos em CSV
│   ├── extractors_contratos_responsaveis.py  # Extração de responsáveis dos contratos
│   ├── transformer_contratos_responsaveis.py # Consolidação de responsáveis em CSV
│   └── logger.py                    # Logging e resumo de execução
├── data/
│   ├── compras.csv                  # dataset consolidado de compras
│   ├── compras_itens.csv            # dataset consolidado de itens
│   ├── atas.csv                     # dataset consolidado de atas
│   ├── atas_itens.csv               # dataset consolidado de itens das atas
│   ├── atas_saldos.csv              # dataset consolidado de saldos das atas
│   ├── atas_unidades.csv            # dataset consolidado de unidades participantes
│   ├── contratos.csv                # dataset consolidado de contratos
│   └── contratos_responsaveis.csv   # dataset consolidado de responsáveis dos contratos
├── temp/
│   ├── compras/                     # Cache JSON das compras por período/modalidade
│   ├── compras_itens/               # Cache JSON dos itens por compra
│   ├── atas/                        # Cache JSON das atas por período
│   ├── atas_itens/                  # Cache JSON dos itens das atas
│   ├── atas_saldos/                 # Cache JSON dos saldos das atas
│   ├── atas_unidades/               # Cache JSON das unidades participantes
│   ├── contratos/                   # Cache JSON dos contratos
│   └── contratos_responsaveis/      # Cache JSON dos responsáveis dos contratos
├── utils/
│   ├── analisar_cobertura_itens.py  # Análise de cobertura de itens
│   ├── analisar_csv.py              # Análise estatística dos CSVs
│   ├── diagnosticar_id.py           # Diagnóstico de IDs duplicados/faltantes
│   ├── explorar_itens.py            # Exploração da estrutura de itens
│   ├── limpar_itens.py              # Limpeza e validação de dados de itens
│   ├── migrar_cache_antigo.py       # Migração de cache antigo
│   ├── recuperar_cache.py           # Recuperação de dados de cache corrompidos
│   └── analise.txt                  # Arquivo de análise
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

Executa toda a sequência: extração de compras, itens, atas e seus subcomponentes, seguida de consolidação em CSV.

```bash
python main.py
```

**Fluxo:**
1. Extrai compras — módulo Legado → `temp/compras/`
2. Extrai compras — módulo Lei 14.133 → `temp/compras/`
3. Consolida JSONs → `data/compras.csv`
4. Extrai itens de cada compra → `temp/compras_itens/`
5. Consolida itens → `data/compras_itens.csv`
6. Extrai atas de registro de preço → `temp/atas/`
7. Consolida atas → `data/atas.csv`
8. Extrai itens das atas → `temp/atas_itens/`
9. Consolida itens das atas → `data/atas_itens.csv`
10. Extrai saldos das atas → `temp/atas_saldos/`
11. Consolida saldos → `data/atas_saldos.csv`
12. Extrai unidades participantes → `temp/atas_unidades/`
13. Consolida unidades → `data/atas_unidades.csv`

> **Nota:** A extração de contratos e responsáveis é feita por módulos separados (não faz parte do pipeline padrão). Use os modos específicos abaixo.

#### Modos Específicos

Execute apenas uma etapa do pipeline:

```bash
# Consolidar compras (sem re-extrair)
python main.py --modo transformer_compras

# Extrair e consolidar itens
python main.py --modo extrator_compras_itens

# Consolidar itens (sem re-extrair)
python main.py --modo transformer_compras_itens

# Extrair e consolidar atas
python main.py --modo extrator_atas

# Consolidar atas (sem re-extrair)
python main.py --modo transformer_atas

# Extrair e consolidar itens das atas
python main.py --modo extrator_atas_itens

# Consolidar itens das atas (sem re-extrair)
python main.py --modo transformer_atas_itens

# Extrair e consolidar saldos das atas
python main.py --modo extrator_atas_saldos

# Consolidar saldos (sem re-extrair)
python main.py --modo transformer_atas_saldos

# Extrair e consolidar unidades participantes
python main.py --modo extrator_atas_unidades

# Consolidar unidades (sem re-extrair)
python main.py --modo transformer_atas_unidades

# Extrair e consolidar contratos
python main.py --modo extrator_contratos

# Consolidar contratos (sem re-extrair)
python main.py --modo transformer_contratos

# Extrair e consolidar responsáveis dos contratos
python main.py --modo extrator_contratos_resp

# Consolidar responsáveis dos contratos (sem re-extrair)
python main.py --modo transformer_contratos_resp
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
- **Atas e Itens/Saldos/Unidades:** 2023 até o ano atual + 1

Ajuste em `CONFIG_APIS['LEGADO']['anos']`, `CONFIG_APIS['LEI14133']['anos']` ou `CONFIG_ATAS['anos']` / `CONFIG_ATAS['anos_itens']`.

### Endpoints

Os endpoints consultados são configuráveis em `CONFIG_APIS` e `CONFIG_ATAS`. Atualmente:

**Módulo Legado:**
- Outras Modalidades (licitações abertas)
- Pregões
- Dispensas

**Lei 14.133 (PNCP):**
- Contratações por modalidade

**Atas de Registro de Preço:**
- Consolidado nacional (filtrando por UASG)
- Itens das atas
- Saldos remanescentes
- Unidades participantes

**Contratos / Responsáveis (contratos.comprasnet.gov.br):**
- Contratos
- Responsáveis pelos contratos

### Flags de execução condicional

Você pode controlar se os módulos mais pesados devem ser executados no pipeline completo diretamente em `config/config.py`:

- `CONFIG_APIS["LEGADO"]["executar_legado"] = False` → pula a extração de compras do módulo Legado.
- `CONFIG_ATAS["executar_saldos"] = False` → pula a extração/transformação de saldos das atas.

Para rodar novamente esses módulos, altere a flag para `True` e execute `python main.py`.

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

### Itens das Atas

Produtos/serviços registrados nas atas.

**Campos principais:**
- Número da ata, item ID
- Descrição, quantidade, valor
- Fornecedor

### Saldos das Atas

Saldos remanescentes dos itens nas atas.

**Campos principais:**
- Número da ata, item ID
- Saldo quantidade, saldo valor
- Unidade gestora

### Unidades Participantes

Unidades gestoras que participam das atas.

**Campos principais:**
- Número da ata
- Código UASG, nome unidade
- Percentual participação

### Contratos

Dados obtidos da API `https://contratos.comprasnet.gov.br/api`.
A documentação pode ser consultada em `https://contratos.comprasnet.gov.br/api/docs`

**Campos principais:**
- Contrato ID, número do contrato, objeto, valor
- Datas (assinatura, vigência, término)
- Unidade gestora
- Fornecedor

### Responsáveis dos Contratos

Dados relacionados aos responsáveis pela execução dos contratos.

**Campos principais:**
- Contrato ID
- Nome do responsável
- CPF/CNPJ
- Papel/Função

## 🔄 Cache e Performance

O pipeline utiliza **cache em disco** para evitar re-consultas desnecessárias:

- Arquivos JSON são armazenados em `temp/compras/`, `temp/compras_itens/`, `temp/atas/`, etc.
- Cache é validado por status (marcado como `SUCESSO` ou `FALHA`)
- Dados com sucesso não são sobrescritos por falhas

Para limpar o cache e re-extrair tudo, delete os arquivos JSON de `temp/`.

## 📈 Scripts Auxiliares

Em `utils/` há vários scripts de análise:

| Script | Descrição |
|--------|-----------|
| `analisar_csv.py` | Analisa estatísticas dos CSVs gerados |
| `analisar_cobertura_itens.py` | Verifica cobertura de itens por compra |
| `diagnosticar_id.py` | Diagnóstico de IDs duplicados/faltantes |
| `explorar_itens.py` | Exploração de estrutura de itens |
| `limpar_itens.py` | Limpeza e validação de dados de itens |
| `recuperar_cache.py` | Recuperação de dados de cache corrompidos |
| `migrar_cache_antigo.py` | Migração de cache de versões anteriores |

Exemplo:

```bash
python utils/analisar_csv.py
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
- Verifique se há dados em `temp/compras/`, `temp/compras_itens/` ou `temp/atas/`
- Use scripts de análise: `python utils/analisar_csv.py`

## 📞 Suporte

Para dúvidas sobre a API de dados abertos, consulte:
- Documentação API: https://dadosabertos.compras.gov.br/swagger-ui/index.html

## 📄 Licença

Este projeto está associado ao Instituto Federal de Mato Grosso do Sul (IFMS).

