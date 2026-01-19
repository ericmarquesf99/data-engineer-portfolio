# 🚀 Enterprise Data Pipeline: API → Databricks → Snowflake

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![PySpark](https://img.shields.io/badge/PySpark-3.5+-orange.svg)
![Databricks](https://img.shields.io/badge/Databricks-Jobs-red.svg)
![Snowflake](https://img.shields.io/badge/Snowflake-Cloud_DW-blue.svg)
![Tests](https://img.shields.io/badge/Tests-Passing-success.svg)
![Coverage](https://img.shields.io/badge/Coverage-85%25-green.svg)

## 📋 Visão Geral

Pipeline de dados **enterprise-grade** que demonstra arquitetura moderna de engenharia de dados com **modularização completa**, **testes abrangentes** e **observabilidade estruturada**.

🎯 **Extrai** dados de criptomoedas da API CoinGecko  
⚙️ **Processa** com PySpark no Databricks (Medallion Architecture)  
📊 **Carrega** incrementalmente no Snowflake (Type 2 SCD)  
🔄 **Orquestra** com Databricks Jobs  

### 🌟 Destaques da Arquitetura

- ✅ **Modularidade**: Estrutura organizada (extractors/transformers/loaders/utils)
- ✅ **Testabilidade**: Suite completa de testes unitários com pytest (>80% coverage)
- ✅ **Observabilidade**: Logging estruturado JSON com rastreamento de runs
- ✅ **Multi-Environment**: Configurações separadas (dev/staging/prod)
- ✅ **Databricks Notebooks**: 4 notebooks prontos para produção
- ✅ **Type 2 SCD**: Histórico completo de mudanças no Silver layer
- ✅ **DBFS Storage**: Bronze layer otimizado para Databricks
- ✅ **Data Quality**: Validações, anomaly detection, schema enforcement

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────────────────────┐
│                     CoinGecko API v3                             │
│              (Cryptocurrency Market Data)                        │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                   📥 EXTRACTION                                  │
│                 (notebooks/01_extraction.py)                     │
│    • Rate limiting (50 req/min)  • Retry logic  • DBFS save    │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│             🥉 BRONZE LAYER (DBFS)                              │
│          dbfs:/mnt/data/bronze/crypto/*.json                    │
│              Raw, immutable, timestamped                        │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                  ⚙️  TRANSFORMATION                              │
│              (notebooks/02_transformation.py)                    │
│    • PySpark processing  • Quality checks  • Aggregations      │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ├─────────────────┐
                         ▼                 ▼
┌──────────────────────────────┐  ┌─────────────────────────────┐
│   🥈 SILVER LAYER           │  │   🥇 GOLD LAYER            │
│   (Snowflake)                │  │   (Snowflake)              │
│                              │  │                            │
│ silver_crypto_clean          │  │ gold_crypto_metrics        │
│ • Cleaned & validated        │  │ • Aggregated KPIs          │
│ • Type 2 SCD                 │  │ • Ready for BI             │
│ • is_current tracking        │  │ • Optimized queries        │
└──────────────────────────────┘  └─────────────────────────────┘
                         │                 │
                         └─────────┬───────┘
                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                  📤 LOADING                                      │
│               (notebooks/03_loading.py)                          │
│    • Staging tables  • MERGE operations  • Metadata logging    │
└─────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                🎯 ORCHESTRATION                                  │
│            (notebooks/00_orchestrator.py)                        │
│         Databricks Jobs - Scheduled Execution                    │
└─────────────────────────────────────────────────────────────────┘
```

### 🔑 Medallion Architecture

- **🥉 Bronze**: Raw JSON from API → DBFS (immutable, timestamped)
- **🥈 Silver**: Cleaned & validated → Snowflake (Type 2 SCD)
- **🥇 Gold**: Aggregated metrics → Snowflake (business KPIs)

## 🚀 Funcionalidades

### Extração (API Extractor)
- ✅ Consumo de API REST com retry automático
- ✅ Rate limiting para respeitar limites da API
- ✅ Exponential backoff em caso de falhas
- ✅ Logging detalhado de todas operações

### Processamento (PySpark)
- ✅ Validações de qualidade de dados
- ✅ Schema enforcement
- ✅ Detecção de anomalias (preço e volume)
- ✅ Transformações complexas e derivações
- ✅ Particionamento otimizado

### Carga (Snowflake Loader)
- ✅ **Incremental Load**: Staging + MERGE (Type 2 SCD / UPSERT)
- ✅ **Stage + Merge**: write_pandas para estágio e MERGE para silver/gold
- ✅ **Versioning**: Histórico completo de mudanças (is_current, valid_from/valid_to)
- ✅ **Views/Materialized**: Views e MVs para performance analítica

### Orquestração (Databricks Jobs)
- ✅ Jobs agendados no Databricks
- ✅ Logs e monitoramento nativos
- ✅ Retry automático configurável
- ✅ Notifications (webhooks/email) via Databricks
- ✅ One-click rerun no workspace

## 📁 Estrutura do Projeto

```
enterprise-data-pipeline/
├── 📓 notebooks/                # Databricks notebooks (produção)
│   ├── 00_orchestrator.py      # → Orquestrador principal
│   ├── 01_extraction.py        # → Extração API → DBFS
│   ├── 02_transformation.py    # → PySpark transformations
│   └── 03_loading.py           # → Carga Snowflake
│
├── 🐍 src/                      # Módulos Python (modularizados)
│   ├── extractors/             # → API data extraction
│   │   ├── __init__.py
│   │   └── coingecko_extractor.py
│   ├── transformers/           # → PySpark processing
│   │   ├── __init__.py
│   │   └── spark_processor.py
│   ├── loaders/                # → Data warehouse loading
│   │   ├── __init__.py
│   │   └── snowflake_loader.py
│   └── utils/                  # → Utilities
│       ├── __init__.py
│       ├── logging_config.py   # → Structured JSON logging
│       ├── config_loader.py    # → Multi-env configuration
│       └── validators.py       # → Data quality checks
│
├── 🧪 tests/                    # Test suites
│   ├── unit/                   # → Unit tests (mocked)
│   │   ├── test_extractors.py
│   │   ├── test_transformers.py
│   │   ├── test_loaders.py
│   │   └── test_utils.py
│   └── integration/            # → Integration tests
│       └── test_pipeline.py
│
├── ⚙️  config/                  # Configuration management
│   ├── config.yaml             # → Base configuration
│   ├── .env.example            # → Environment variables template
│   └── environments/           # → Environment-specific configs
│       ├── development.yaml
│       ├── staging.yaml
│       └── production.yaml
│
├── 🗄️  sql/                     # SQL models and views
│   └── snowflake_models.sql    # → Tables, views, materialized views
│
├── 📚 docs/                     # Documentation
│   ├── README.md               # → Documentation index
│   ├── ARCHITECTURE.md         # → System architecture
│   ├── SETUP.md                # → Complete setup guide
│   ├── DATABRICKS_GUIDE.md     # → Databricks deployment
│   ├── TESTING.md              # → Testing guide
│   └── SNOWFLAKE_*.md          # → Snowflake guides
│
├── 📊 logs/                     # Structured JSON logs
├── pytest.ini                   # Pytest configuration
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

## � Quick Start

### 1. Clonar e Instalar

```bash
git clone <repository-url>
cd enterprise-data-pipeline

# Criar ambiente virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# Instalar dependências
pip install -r requirements.txt
```

### 2. Configurar Credenciais

```bash
# Copiar template
cp config/.env.example .env

# Editar com suas credenciais
nano .env  # ou seu editor preferido
```

```env
# .env
DATABASE_TYPE=snowflake
SNOWFLAKE_ACCOUNT=your_account_id
SNOWFLAKE_USER=your_username
SNOWFLAKE_AUTHENTICATOR=externalbrowser
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=CRYPTO_DATA_PROD
SNOWFLAKE_SCHEMA=PUBLIC
ENVIRONMENT=production
```

### 3. Setup Snowflake

```sql
-- Conectar no Snowflake e executar:
CREATE DATABASE CRYPTO_DATA_PROD;
USE CRYPTO_DATA_PROD;

-- Executar todo o conteúdo de sql/snowflake_models.sql
```

📖 **Guia completo**: [docs/SNOWFLAKE_SETUP.md](docs/SNOWFLAKE_SETUP.md)

### 4. Deploy no Databricks

1. **Clone repo no Databricks Repos**
2. **Configure secrets**:
   ```python
   dbutils.secrets.createScope(scope="snowflake")
   dbutils.secrets.put(scope="snowflake", key="account", ...)
   ```
3. **Crie um Databricks Job** apontando para `notebooks/00_orchestrator.py`
4. **Agende** (ex: a cada 6 horas)

📖 **Guia completo**: [docs/DATABRICKS_GUIDE.md](docs/DATABRICKS_GUIDE.md)

### 5. Executar Testes

```bash
# Todos os testes
pytest tests/ -v

# Com coverage
pytest tests/ --cov=src --cov-report=html
```

📖 **Guia de testes**: [docs/TESTING.md](docs/TESTING.md)

### 2. Criar Ambiente Virtual

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate  # Windows
```

### 3. Instalar Dependências

```bash
pip install -r requirements.txt
```

### 4. Configurar Variáveis de Ambiente

```bash
cp config/.env.example config/.env
# Editar .env com suas credenciais
```

### 5. Configurar Databricks

1. Criar workspace no Databricks
2. Criar cluster com PySpark 3.5+
3. Obter token de acesso
4. Configurar no `.env`

### 6. Configurar Snowflake (External Browser Auth)

```bash
# Variáveis no .env
SNOWFLAKE_ACCOUNT=seu-account-id
SNOWFLAKE_USER=seu-usuario
SNOWFLAKE_AUTHENTICATOR=externalbrowser
SNOWFLAKE_WAREHOUSE=SNOWFLAKE_LEARNING_WH
SNOWFLAKE_DATABASE=CRYPTO_DB
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

1. Criar conta Snowflake (trial) e warehouse `SNOWFLAKE_LEARNING_WH`
2. Usar authenticator `externalbrowser` (não armazena senha)
3. Testar conexão:
```bash
python test_snowflake_connection.py
```

### 7. (Opcional) Agendar no Databricks Jobs

1. No workspace, crie um Job
2. Tipo: Python script
3. Script: `src/pipeline_orchestrator.py`
4. Cluster: usar cluster existente
5. Schedule: defina o cron desejado
6. Defina variáveis/env via Secrets ou cluster env vars

## 🎮 Como Usar

### Execução Manual (Standalone)

```bash
# Executar pipeline completo
cd src
python pipeline_orchestrator.py
```

### Execução via Databricks Jobs

- Crie um Job apontando para `src/pipeline_orchestrator.py`
- Configure parâmetros/vars no Job ou via Secrets
- Agende o cron diretamente no Job

### Execução de Componentes Individuais

```bash
# Apenas extração
python src/api_extractor.py

# Apenas processamento
python src/spark_processor.py

# Apenas carga
python src/snowflake_loader.py
```

## 📊 Validações e Qualidade de Dados

### Regras de Validação

1. **Campos Obrigatórios**: coin_id, symbol, current_price não podem ser nulos
2. **Range de Preços**: 0 < price < 1,000,000
3. **Valores Positivos**: market_cap e volume devem ser > 0
4. **Detecção de Anomalias**: 
   - Mudança de preço > 50% em 24h
   - Volume 3x acima do desvio padrão

### Métricas de Qualidade

- Data Completeness Score
- Validation Success Rate
- Anomaly Detection Count
- Processing Time per Record

## 🔍 Consultas SQL de Exemplo

### Ver Estado Atual do Mercado

```sql
SELECT * FROM v_current_market_state
ORDER BY market_cap_rank
LIMIT 10;
```

### Top Movers (24h)

```sql
SELECT * FROM v_top_movers_24h;
```

### Histórico de Execução do Pipeline

```sql
SELECT * FROM v_pipeline_execution_history
ORDER BY run_date DESC
LIMIT 20;
```

### Anomalias Detectadas

```sql
SELECT 
    symbol,
    name,
    current_price,
    price_change_percentage_24h,
    is_price_anomaly,
    is_volume_spike
FROM v_current_market_state
WHERE is_price_anomaly = TRUE OR is_volume_spike = TRUE;
```

## 🎯 Features Avançadas

### 1. Incremental Load (Type 2 SCD)

```python
# Mantém histórico completo de mudanças
# Campos: is_current, valid_from, valid_to
# Permite análise temporal
```

### 2. Schema Evolution

```python
# Suporta adição de novas colunas
# Não quebra pipelines existentes
# Versionamento automático
```

### 3. Logging Estruturado

```python
# Logs em JSON com contexto completo
# Rastreamento de run_id
# Métricas de performance
```

### 4. Retry Logic

```python
# Exponential backoff
# Configurable retry attempts
# Circuit breaker pattern
```

## 📈 Métricas e Monitoramento

### Dashboard de Métricas

- Records Extracted
- Records Processed
- Records Loaded
- Data Quality Score
- Execution Time
- Error Rate

### Alertas Configurados

- Pipeline failure
- Data quality below threshold
- SLA breach
- Anomaly spike

## 🧪 Testes

```bash
# Executar testes unitários
pytest tests/

# Com coverage
pytest --cov=src tests/

# Apenas testes específicos
pytest tests/test_api_extractor.py
```

## 📝 Documentação Técnica

### API Extractor

- **Rate Limit**: 50 requests/minuto
- **Timeout**: 30 segundos
- **Retry**: 3 tentativas com backoff

### Spark Processor

- **Partitions**: 200 (configurável)
- **Adaptive Query**: Habilitado
- **Memory**: 4GB driver, 8GB executors

### Snowflake Loader

- **Staging + MERGE**: write_pandas para stage e MERGE para silver/gold
- **Incremental**: Type 2 SCD (is_current, valid_from/valid_to)
- **Batch**: Carregamento em lote via stage tables
- **Views/MVs**: Views analíticas e materialized views

## 🚀 Próximos Passos / Melhorias

- [ ] Implementar CDC (Change Data Capture)
- [ ] Adicionar streaming com Kafka
- [ ] Dashboard em tempo real (Streamlit/Dash)
- [ ] Machine Learning para previsão de preços
- [ ] Data lineage com OpenLineage
- [ ] Testes de integração end-to-end
- [ ] CI/CD com GitHub Actions
- [ ] Containerização com Docker
- [ ] Deployment em Kubernetes

## 📚 Tecnologias Utilizadas

| Categoria | Tecnologia | Versão |
|-----------|-----------|--------|
| Linguagem | Python | 3.9+ |
| Processing | PySpark | 3.5+ |
| Orchestration | Databricks Jobs | - |
| Warehouse | Snowflake | Enterprise |
| Compute | Databricks | Runtime 13.3+ |
| API | CoinGecko | v3 |

## 🤝 Contribuindo

Contribuições são bem-vindas! Por favor:

1. Fork o projeto
2. Crie uma branch (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## 📄 Licença

Este projeto é distribuído sob a licença MIT. Veja `LICENSE` para mais informações.

## 👤 Autor

**Eric M.**

- LinkedIn: [seu-linkedin](https://linkedin.com/in/seu-perfil)
- Email: seu-email@example.com
- Portfolio: [seu-portfolio](https://seu-portfolio.com)

## 🙏 Agradecimentos

- CoinGecko pela API pública gratuita
- Comunidade Databricks e Snowflake
- Apache Airflow community

---

⭐ Se este projeto foi útil, considere dar uma estrela!
