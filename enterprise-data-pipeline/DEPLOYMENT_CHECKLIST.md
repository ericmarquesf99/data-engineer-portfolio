# ✅ Deployment Checklist

Checklist completo para deploy em produção do Enterprise Data Pipeline.

## 📋 Pré-Deployment

### Ambiente Local

- [ ] **Virtual environment criado e ativado**
  ```bash
  python -m venv venv
  source venv/bin/activate  # ou venv\Scripts\activate
  ```

- [ ] **Dependências instaladas**
  ```bash
  pip install -r requirements.txt
  ```

- [ ] **Testes passando localmente**
  ```bash
  pytest tests/ -v
  ```

- [ ] **Imports funcionando**
  ```bash
  python -c "from src.extractors.coingecko_extractor import CryptoExtractor; print('OK')"
  ```

- [ ] **Arquivo .env configurado**
  ```bash
  cp config/.env.example .env
  # Editar .env com credenciais reais
  ```

---

## 🗄️ Snowflake Setup

- [ ] **Conta Snowflake ativa**
  - [ ] Trial ou conta paga
  - [ ] Warehouse criado (COMPUTE_WH)

- [ ] **Database criado**
  ```sql
  CREATE DATABASE CRYPTO_DATA_PROD;
  ```

- [ ] **Schema criado**
  ```sql
  USE DATABASE CRYPTO_DATA_PROD;
  CREATE SCHEMA IF NOT EXISTS PUBLIC;
  ```

- [ ] **Tabelas criadas**
  ```sql
  -- Executar todo o conteúdo de sql/snowflake_models.sql
  ```

- [ ] **Permissões configuradas**
  ```sql
  GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE SYSADMIN;
  GRANT USAGE ON DATABASE CRYPTO_DATA_PROD TO ROLE SYSADMIN;
  GRANT ALL ON SCHEMA CRYPTO_DATA_PROD.PUBLIC TO ROLE SYSADMIN;
  ```

- [ ] **Conexão testada**
  ```bash
  python test_snowflake_connection.py
  ```

- [ ] **Query teste executada**
  ```sql
  SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();
  SHOW TABLES;
  ```

---

## ☁️ Databricks Setup

### Workspace

- [ ] **Conta Databricks criada**
  - [ ] Community Edition OU
  - [ ] Trial/Paid account

- [ ] **Workspace acessível**
  - [ ] Login funcionando
  - [ ] Interface carregando

### Cluster

- [ ] **Cluster criado**
  - [ ] Nome: `crypto-pipeline-cluster`
  - [ ] Runtime: 13.3 LTS ou superior
  - [ ] Node Type: Standard_DS3_v2 ou similar
  - [ ] Workers: 2-4 (auto-scaling)

- [ ] **Bibliotecas instaladas**
  - [ ] `snowflake-connector-python[pandas]`
  - [ ] `pyyaml`
  - [ ] `tenacity`
  - [ ] `python-dotenv`

- [ ] **Cluster iniciado**
  - [ ] Status: Running
  - [ ] Spark UI acessível

### Repositório

- [ ] **GitHub Repo conectado**
  - [ ] Workspace → Repos → Add Repo
  - [ ] URL do GitHub configurada
  - [ ] Branch: main

- [ ] **Arquivos sincronizados**
  - [ ] `notebooks/` visível
  - [ ] `src/` visível
  - [ ] `config/` visível

- [ ] **Paths corrigidos nos notebooks**
  ```python
  # Em cada notebook, atualizar:
  sys.path.append("/Workspace/Repos/SEU-USERNAME/enterprise-data-pipeline/src")
  ```

### Secrets

- [ ] **Secret scope criado**
  ```python
  dbutils.secrets.createScope(scope="snowflake")
  ```

- [ ] **Secrets adicionados**
  - [ ] `account`
  - [ ] `user`
  - [ ] `password` (se não usar externalbrowser)
  - [ ] `warehouse`
  - [ ] `database`
  - [ ] `schema`

- [ ] **Secrets testados**
  ```python
  # Em notebook:
  account = dbutils.secrets.get(scope="snowflake", key="account")
  print(f"Account: {account[:3]}...")  # Mostra apenas primeiros 3 chars
  ```

### DBFS

- [ ] **Path Bronze criado**
  ```python
  dbutils.fs.mkdirs("dbfs:/mnt/data/bronze/crypto/")
  ```

- [ ] **Permissões verificadas**
  ```python
  dbutils.fs.ls("dbfs:/mnt/data/bronze/")
  ```

### Notebooks

- [ ] **01_extraction.py testado**
  - [ ] Anexado ao cluster
  - [ ] Parâmetros configurados
  - [ ] Executado sem erros
  - [ ] Arquivo criado no DBFS

- [ ] **02_transformation.py testado**
  - [ ] Input path configurado
  - [ ] Executado sem erros
  - [ ] Views temporárias criadas

- [ ] **03_loading.py testado**
  - [ ] Snowflake params configurados
  - [ ] Executado sem erros
  - [ ] Dados visíveis no Snowflake

- [ ] **00_orchestrator.py testado**
  - [ ] Todos os notebooks chamados
  - [ ] Execução completa
  - [ ] Resultado JSON retornado

---

## 🔄 Databricks Job

### Configuração

- [ ] **Job criado**
  - [ ] Workflows → Create Job
  - [ ] Nome: `Crypto Data Pipeline`

- [ ] **Task configurado**
  - [ ] Task Name: `Orchestrate Pipeline`
  - [ ] Type: Notebook
  - [ ] Path: `/Repos/username/enterprise-data-pipeline/notebooks/00_orchestrator`

- [ ] **Cluster configurado**
  - [ ] Existing cluster OU
  - [ ] New job cluster (recomendado para prod)

- [ ] **Schedule configurado**
  - [ ] Cron expression correto (ex: `0 */6 * * *`)
  - [ ] Timezone correto
  - [ ] Status: Active

- [ ] **Alerts configurados**
  - [ ] Email on failure: SIM
  - [ ] Email: seu-email@exemplo.com

- [ ] **Advanced settings**
  - [ ] Timeout: 3600 seconds
  - [ ] Max Retries: 2
  - [ ] Retry Interval: 300 seconds

### Teste

- [ ] **Execução manual**
  - [ ] Click "Run Now"
  - [ ] Job iniciou

- [ ] **Monitoramento**
  - [ ] Runs tab acessível
  - [ ] Logs visíveis
  - [ ] Output JSON correto

- [ ] **Verificação Snowflake**
  ```sql
  SELECT COUNT(*) FROM silver_crypto_clean;
  SELECT COUNT(*) FROM gold_crypto_metrics;
  SELECT * FROM pipeline_metadata ORDER BY execution_timestamp DESC LIMIT 1;
  ```

---

## 🧪 Validação

### Dados

- [ ] **Bronze layer**
  - [ ] Arquivos JSON no DBFS
  - [ ] Timestamps corretos
  - [ ] Estrutura JSON válida

- [ ] **Silver layer**
  - [ ] Registros no Snowflake
  - [ ] is_current corretamente configurado
  - [ ] valid_from/valid_to funcionando
  - [ ] Sem duplicatas

- [ ] **Gold layer**
  - [ ] Métricas agregadas corretas
  - [ ] Valores fazem sentido
  - [ ] Datas atualizadas

### Quality Checks

- [ ] **Completude**
  - [ ] Campos obrigatórios preenchidos
  - [ ] Null percentage < 5%

- [ ] **Consistência**
  - [ ] Preços > 0
  - [ ] Market caps razoáveis
  - [ ] Timestamps recentes

- [ ] **Duplicatas**
  - [ ] Nenhuma duplicata ativa (is_current=TRUE)
  - [ ] Histórico preservado

### Logs

- [ ] **Structured logs**
  - [ ] Logs em JSON
  - [ ] run_id presente
  - [ ] Events registrados

- [ ] **Métricas**
  - [ ] Records processed
  - [ ] Duration
  - [ ] Success/failure

---

## 📊 Monitoramento

### Databricks

- [ ] **Job monitoring ativo**
  - [ ] Email alerts funcionando
  - [ ] Run history visível

- [ ] **Cluster monitoring**
  - [ ] Spark UI acessível
  - [ ] Métricas de performance

### Snowflake

- [ ] **Query history**
  - [ ] Queries visíveis
  - [ ] Performance aceitável

- [ ] **Warehouse usage**
  - [ ] Credits usage razoável
  - [ ] Auto-suspend configurado

### Alertas

- [ ] **Failure alerts**
  - [ ] Notificações funcionando
  - [ ] Email recebido em teste

- [ ] **Data quality alerts** (opcional)
  - [ ] Threshold configurado
  - [ ] Alertas testados

---

## 📝 Documentação

- [ ] **README.md atualizado**
  - [ ] Badges corretos
  - [ ] Links funcionando

- [ ] **Docs organizados**
  - [ ] docs/ folder populado
  - [ ] Links internos funcionando

- [ ] **Runbook criado** (opcional)
  - [ ] Procedimentos operacionais
  - [ ] Troubleshooting

---

## 🔒 Segurança

- [ ] **Credenciais seguras**
  - [ ] Não commitadas no Git
  - [ ] Secrets no Databricks
  - [ ] .env no .gitignore

- [ ] **Permissões mínimas**
  - [ ] Apenas permissões necessárias
  - [ ] Roles apropriados

- [ ] **Network security**
  - [ ] HTTPS para APIs
  - [ ] Conexões criptografadas

---

## 🚀 Go-Live

### Final Checks

- [ ] **Smoke test completo**
  - [ ] Executar pipeline manualmente
  - [ ] Verificar dados
  - [ ] Validar métricas

- [ ] **Backup configs**
  - [ ] Commit no Git
  - [ ] Tag de release (v1.0.0)

- [ ] **Comunicação**
  - [ ] Stakeholders notificados
  - [ ] Documentação compartilhada

### Ativação

- [ ] **Schedule ativado**
  - [ ] Job schedule: ON
  - [ ] Primeiro run agendado visível

- [ ] **Monitoring ativo**
  - [ ] Alerts configurados
  - [ ] Dashboard pronto (se aplicável)

---

## 📅 Pós-Deploy

### Primeiras 24h

- [ ] **Monitorar primeira execução**
  - [ ] Job executou no horário
  - [ ] Sem erros
  - [ ] Dados corretos

- [ ] **Verificar custos**
  - [ ] Databricks usage
  - [ ] Snowflake credits

### Primeira Semana

- [ ] **Performance review**
  - [ ] Job duration aceitável
  - [ ] Sem timeouts
  - [ ] Dados consistentes

- [ ] **Ajustes** (se necessário)
  - [ ] Cluster size
  - [ ] Schedule frequency
  - [ ] Retention policies

### Primeiro Mês

- [ ] **Retrospectiva**
  - [ ] Lições aprendidas
  - [ ] Documentação atualizada
  - [ ] Melhorias identificadas

---

## 🎯 Success Criteria

### Funcionalidade
✅ Pipeline executa end-to-end sem erros  
✅ Dados chegam no Snowflake  
✅ Quality checks passando  
✅ Logs estruturados gerados  

### Performance
✅ Job completa em < 30 minutos  
✅ Sem timeout errors  
✅ Custos dentro do esperado  

### Confiabilidade
✅ Retries funcionando  
✅ Alertas notificando falhas  
✅ Dados históricos preservados  

---

## 📞 Contatos de Suporte

**Databricks Support**: [Link do portal]  
**Snowflake Support**: [Link do portal]  
**Internal Team**: [Contato da equipe]  

---

**✨ Deployment completo! Pipeline em produção!**
