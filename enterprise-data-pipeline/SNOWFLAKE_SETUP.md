# 🔧 Integração Snowflake - Guia de Configuração

## ⚠️ SEGURANÇA PRIMEIRO

Você compartilhou suas credenciais reais no chat. **FAÇA ISTO AGORA:**

### 1. Rotear Senha Snowflake (URGENTE!)

```
Snowflake Account (na web) → Admin → Users & Roles
  → Seu usuário (ERICMARQUESF)
  → Change Password
```

**Por que?** Qualquer pessoa com seu account ID e user pode tentar acessar sua conta.

---

## 🔐 Configurar Variáveis de Ambiente

### Opção 1: External Browser Auth (SEGURO) ⭐

Você já usa `externalbrowser`, o que é **mais seguro**. Não guarda senha!

```bash
# config/.env
SNOWFLAKE_ACCOUNT=EYZZSXW-IR02741
SNOWFLAKE_USER=ERICMARQUESF
SNOWFLAKE_AUTHENTICATOR=externalbrowser
SNOWFLAKE_WAREHOUSE=SNOWFLAKE_LEARNING_WH
SNOWFLAKE_DATABASE=CRYPTO_DB
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

### Opção 2: Password Auth (Se preferir)

```bash
# config/.env
SNOWFLAKE_ACCOUNT=EYZZSXW-IR02741
SNOWFLAKE_USER=ERICMARQUESF
SNOWFLAKE_PASSWORD=sua_nova_senha  # ⚠️ NUNCA commitar no Git!
SNOWFLAKE_AUTHENTICATOR=username_password_mfa
SNOWFLAKE_WAREHOUSE=SNOWFLAKE_LEARNING_WH
SNOWFLAKE_DATABASE=CRYPTO_DB
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

---

## 📝 Atualizar config.yaml

```yaml
snowflake:
  account: ${SNOWFLAKE_ACCOUNT}
  user: ${SNOWFLAKE_USER}
  authenticator: externalbrowser  # ou username_password_mfa
  password: ${SNOWFLAKE_PASSWORD}  # Deixar em branco se usar externalbrowser
  warehouse: ${SNOWFLAKE_WAREHOUSE}
  database: ${SNOWFLAKE_DATABASE}
  schema: ${SNOWFLAKE_SCHEMA}
  role: ${SNOWFLAKE_ROLE}
  
  tables:
    silver: silver_crypto_clean
    gold: gold_crypto_metrics
    metadata: pipeline_metadata
```

---

## 🚀 Testar Conexão

### 1. Instalar driver Snowflake

```bash
pip install snowflake-connector-python
```

### 2. Executar teste de conexão

```python
# test_snowflake.py
import snowflake.connector
import os

conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    user=os.getenv('SNOWFLAKE_USER'),
    authenticator='externalbrowser',  # Abre browser para auth
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA'),
    role=os.getenv('SNOWFLAKE_ROLE')
)

cursor = conn.cursor()
cursor.execute("SELECT current_user(), current_warehouse()")
result = cursor.fetchone()
print(f"✅ Conectado como: {result[0]} na warehouse: {result[1]}")

conn.close()
```

```bash
python test_snowflake.py
# Abre browser → Clique em "Click to authenticate" → Volta para terminal
# Output esperado: ✅ Conectado como: ERICMARQUESF na warehouse: SNOWFLAKE_LEARNING_WH
```

---

## 🔄 Usar Snowflake em Vez de PostgreSQL

### Opção 1: Trocar no Orchestrador

```python
# src/pipeline_orchestrator.py

from postgres_loader import PostgresLoader
from snowflake_loader import SnowflakeLoader

config = load_config()

# Escolher loader baseado em ambiente
database_type = os.getenv('DATABASE_TYPE', 'postgres')

if database_type == 'snowflake':
    loader = SnowflakeLoader(config)
else:
    loader = PostgresLoader(config)

# Resto do código fica igual
loader.connect()
loader.setup_database()
# ... processar e carregar dados
loader.close()
```

### Opção 2: Variável de Ambiente

```bash
# .env
DATABASE_TYPE=snowflake  # ou postgres
```

---

## 📊 Comandos SQL Úteis no Snowflake

```sql
-- Ver databases
SHOW DATABASES;

-- Ver schemas
SHOW SCHEMAS IN DATABASE CRYPTO_DB;

-- Ver tabelas criadas
SHOW TABLES IN SCHEMA CRYPTO_DB.PUBLIC;

-- Query rápida
SELECT * FROM SILVER_CRYPTO_CLEAN LIMIT 10;

-- Ver metadata
SELECT * FROM PIPELINE_METADATA ORDER BY CREATED_AT DESC;

-- Query de histórico (requer query ID)
SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
LIMIT 10;
```

---

## 🎯 Fluxo Recomendado

### 1️⃣ Desenvolvimento Local (Grátis)

```bash
# Rodar tudo localmente
docker run -d --name postgres-db -p 5432:5432 postgres:16
python pipeline_orchestrator.py  # Usa PostgreSQL
```

### 2️⃣ Teste com Snowflake (Grátis por 30 dias)

```bash
# Usar Snowflake para demonstração
export DATABASE_TYPE=snowflake
python pipeline_orchestrator.py  # Usa Snowflake
```

### 3️⃣ Produção (Escolher um)

```bash
# Opção A: PostgreSQL (Gratuito)
export DATABASE_TYPE=postgres
docker run -d --name postgres-db -p 5432:5432 postgres:16

# Opção B: Snowflake ($5-600/mês dependendo do uso)
export DATABASE_TYPE=snowflake
# Mantém warehouse sempre ligado ou desliga após cada run
```

---

## 💡 Dicas de Segurança

### ✅ SEMPRE fazer isso:

1. **Credenciais em .env, NUNCA em código**
   ```bash
   # .env (git-ignored)
   SNOWFLAKE_PASSWORD=sua_senha
   
   # ❌ NUNCA em código
   password = "sua_senha"  # Não faça!
   ```

2. **Usar .gitignore**
   ```
   .env
   .env.local
   secrets/
   *.pem
   *.key
   ```

3. **Rotear credenciais regularmente**
   ```
   Snowflake → Admin → Users & Roles → Change Password (a cada 30 dias)
   ```

4. **Usar External Browser Auth**
   ```python
   authenticator='externalbrowser'  # Melhor que guardar password
   ```

5. **Limitar permissões**
   ```sql
   -- Criar user com role restrita
   CREATE USER portfolio_user DEFAULT_ROLE = portfolio_role;
   GRANT ALL ON SCHEMA PUBLIC TO ROLE portfolio_role;
   GRANT ALL ON DATABASE CRYPTO_DB TO ROLE portfolio_role;
   ```

### ❌ NUNCA fazer isso:

- Compartilhar account ID + username
- Hardcode senha em código
- Commitar .env com credenciais
- Usar role ACCOUNTADMIN para pipelines
- Deixar warehouse sempre ligado

---

## 🐛 Troubleshooting

### Erro: "Invalid account identifier"
```
SOLUÇÃO: Verificar SNOWFLAKE_ACCOUNT
A conta deve ser: EYZZSXW-IR02741 (sem URL completa)
```

### Erro: "Authentication failed"
```
SOLUÇÃO 1: Se usar externalbrowser
  - Verificar se browser abriu
  - Clicar em "Click here to authenticate"
  
SOLUÇÃO 2: Se usar password
  - Verificar senha rotada
  - Tentar mudar para externalbrowser
```

### Erro: "Warehouse ... does not exist"
```
SOLUÇÃO: Criar warehouse no Snowflake
  Admin → Warehouses → Create
  Nome: SNOWFLAKE_LEARNING_WH
```

### Erro: "Database ... does not exist"
```
SOLUÇÃO: Script cria database automaticamente
  Mas se necessário:
  CREATE DATABASE CRYPTO_DB;
  CREATE SCHEMA CRYPTO_DB.PUBLIC;
```

---

## 📈 Comparar Performance: PostgreSQL vs Snowflake

| Operação | PostgreSQL | Snowflake |
|----------|-----------|-----------|
| **Inserção** | 1-2s | 3-5s (mais overhead) |
| **Query 300 registros** | <100ms | <500ms |
| **Materialized View** | ✅ Nativo | ✅ Nativo (mais caro) |
| **Custo/mês** | $0 | $5-600 |
| **Setup** | 30s | 2min |

**Conclusão:** PostgreSQL é mais rápido para volume pequeno. Snowflake brilha com TBs de dados.

---

## 🎓 Para Entrevistas

**Q: "Por que você avalia ambos?"**
> "Demonstra versatilidade. PostgreSQL para volume pequeno e desenvolvimento local. Snowflake para escala enterprise e quando o volume justifica o custo."

**Q: "Qual você escolheria?"**
> "Depende dos requisitos: <10GB → PostgreSQL (gratuito), >100GB → Snowflake (escala melhor). Para portfólio: PostgreSQL por custo-benefício."

---

## Próximas Ações

1. ✅ **Rotear senha Snowflake** (AGORA!)
2. ✅ **Criar .env** com variáveis
3. ✅ **Testar conexão** com test_snowflake.py
4. ✅ **Atualizar config.yaml**
5. ✅ **Rodar pipeline** com DATABASE_TYPE=snowflake
6. ✅ **Demonstrar em entrevista** ambas as opções

**Pronto para usar Snowflake!** 🚀
