# ✅ Integração Snowflake - Checklist de Ação

## 🚨 SEGURANÇA PRIMEIRO (FAÇA AGORA!)

- [ ] **Rotear sua senha Snowflake**
  ```
  Snowflake Web UI → Admin → Users & Roles → ERICMARQUESF → Change Password
  Cria uma NOVA senha (as credenciais antigas foram expostas)
  ```

- [ ] **Adicionar .env ao .gitignore**
  ```bash
  echo ".env" >> .gitignore
  echo ".env.local" >> .gitignore
  echo "config/.env" >> .gitignore
  ```

---

## 🔧 SETUP (5 minutos)

### 1. Instalar Driver Snowflake

```bash
cd enterprise-data-pipeline
pip install snowflake-connector-python
```

### 2. Criar .env com suas credenciais

```bash
cp config/.env.example config/.env
```

**Editar `config/.env`:**
```bash
# Snowflake
SNOWFLAKE_ACCOUNT=EYZZSXW-IR02741        # Sua conta
SNOWFLAKE_USER=ERICMARQUESF              # Seu usuário
SNOWFLAKE_PASSWORD=sua_nova_senha        # ⚠️ SUA NOVA SENHA (rotada acima)
SNOWFLAKE_AUTHENTICATOR=externalbrowser  # Melhor opção (não guarda senha)
SNOWFLAKE_WAREHOUSE=SNOWFLAKE_LEARNING_WH
SNOWFLAKE_DATABASE=CRYPTO_DB
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_ROLE=ACCOUNTADMIN

# Database
DATABASE_TYPE=snowflake  # ou postgres
```

### 3. Testar Conexão

```bash
# Testar se está tudo funcionando
python test_snowflake_connection.py

# Expected output:
# ✅ Connected as: ERICMARQUESF
# ✅ Warehouse: SNOWFLAKE_LEARNING_WH
# ✅ ALL TESTS PASSED!
```

Se der erro, ver troubleshooting em SNOWFLAKE_SETUP.md

---

## 🚀 RODAR COM SNOWFLAKE (30 segundos)

### Opção 1: Via Variável de Ambiente

```bash
export DATABASE_TYPE=snowflake
python src/pipeline_orchestrator.py
```

### Opção 2: Mudar em config/.env

```bash
# Editar config/.env
DATABASE_TYPE=snowflake

# Rodar
python src/pipeline_orchestrator.py
```

### Opção 3: Criar Snowflake Loader e Usar Diretamente

```python
from src.snowflake_loader import SnowflakeLoader
import yaml

with open('config/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

loader = SnowflakeLoader(config)
loader.connect()
loader.setup_database()
loader.close()
```

---

## 📊 COMPARAR: PostgreSQL vs Snowflake

### Setup Side-by-Side

```bash
# Terminal 1: PostgreSQL (Local)
docker run -d --name postgres-db -p 5432:5432 postgres:16
export DATABASE_TYPE=postgres
python src/pipeline_orchestrator.py

# Terminal 2: Snowflake (Cloud Trial)
export DATABASE_TYPE=snowflake
python src/pipeline_orchestrator.py
```

### Queries Úteis no Snowflake

```sql
-- Conectar ao Snowflake Web UI e executar:

-- Ver dados carregados
SELECT * FROM SILVER_CRYPTO_CLEAN LIMIT 10;

-- Ver métricas
SELECT * FROM GOLD_CRYPTO_METRICS LIMIT 10;

-- Histórico do pipeline
SELECT * FROM PIPELINE_METADATA ORDER BY CREATED_AT DESC;

-- Tamanho dos dados
SELECT TABLE_NAME, BYTES, ROWS 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'PUBLIC'
ORDER BY BYTES DESC;
```

---

## 💡 PRÓXIMAS AÇÕES

### Para Desenvolvimento

- [ ] Manter PostgreSQL para debug local (rápido, gratuito)
- [ ] Usar Snowflake para demonstrações em entrevista

```bash
# Desenvolvimento
export DATABASE_TYPE=postgres
python src/pipeline_orchestrator.py

# Demonstração
export DATABASE_TYPE=snowflake
python src/pipeline_orchestrator.py
```

### Para Portfólio

- [ ] Adicionar ao README que você suporta ambos
- [ ] Documentar switching entre bancos
- [ ] Mostrar em entrevistas: "Trabalho com ambos"

### Para Escalabilidade

- [ ] Se volume crescer (>100GB) → Snowflake
- [ ] Se volume pequeno (<10GB) → PostgreSQL
- [ ] Híbrido? PostgreSQL para dev, Snowflake para prod

---

## 🎯 TALKING POINTS (Para Entrevistas)

### "Qual banco você usa?"

> "Uso PostgreSQL em desenvolvimento por ser rápido e gratuito, e Snowflake em produção quando o volume justifica o custo. Para portfólio, ambos funcionam - demonstro versão PostgreSQL por ser mais acessível, mas posso trocar para Snowflake em 30 segundos."

### "Por que dois bancos?"

> "Mostra que entendo trade-offs: PostgreSQL é ótimo para <10GB e prototipagem. Snowflake brilha em >100GB e análise distribuída. Escolho baseado em requisitos, não em preferência."

### "Qual é mais rápido?"

> "PostgreSQL é mais rápido para queries pequenas (sub-segundo). Snowflake é mais rápido para queries enormes (TB). Para 300 registros, PostgreSQL vence. Para análise em escala, Snowflake."

---

## ⚠️ AVISO IMPORTANTE

### Custos Snowflake

```
FREE TIER (30 dias):
- $400 em créditos gratuitos
- Cobre ~1 mês de uso baixo

APÓS 30 DIAS:
- Warehouse mínimo: ~$2/hora ativado
- Deixar rodando 24/7: ~$48/mês
- Uso otimizado (apenas quando necessário): ~$5-15/mês

DICA: Desligar warehouse quando não usar:
  ALTER WAREHOUSE "SNOWFLAKE_LEARNING_WH" SUSPEND;
```

### Para Minimizar Custos

1. **Usar PostgreSQL em dev** (gratuito)
2. **Usar Snowflake apenas para demos** (30 dias grátis)
3. **Desligar warehouse ao terminar**
   ```sql
   ALTER WAREHOUSE "SNOWFLAKE_LEARNING_WH" SUSPEND;
   ```

---

## 📚 Arquivos Atualizados

```
enterprise-data-pipeline/
├── ✅ SNOWFLAKE_SETUP.md         ← Leia isto!
├── ✅ test_snowflake_connection.py ← Execute isto!
├── ✅ config/config.yaml         ← Adicionado Snowflake
├── ✅ config/.env.example        ← Adicionado variáveis
└── ✅ src/snowflake_loader.py    ← Já existe
```

---

## 🆘 Se Der Erro

### Erro: "Invalid account"
```
Solução: Verificar SNOWFLAKE_ACCOUNT
Formato correto: EYZZSXW-IR02741 (sem https://)
```

### Erro: "Password auth failed"
```
Solução: Usar externalbrowser em vez de password
SNOWFLAKE_AUTHENTICATOR=externalbrowser
```

### Erro: "Warehouse does not exist"
```
Solução: Criar warehouse
Snowflake Web → Admin → Warehouses → Create
Nome: SNOWFLAKE_LEARNING_WH
```

Ver SNOWFLAKE_SETUP.md para troubleshooting completo

---

## ✨ Resumo Rápido

```bash
# 1. Instalar driver (30s)
pip install snowflake-connector-python

# 2. Configurar .env (1min)
cp config/.env.example config/.env
# Editar com suas credenciais

# 3. Testar (30s)
python test_snowflake_connection.py

# 4. Rodar (30s)
export DATABASE_TYPE=snowflake
python src/pipeline_orchestrator.py

# TOTAL: 3 minutos!
```

---

**🚀 Pronto para usar Snowflake!**

Próximo passo: Ver SNOWFLAKE_SETUP.md para detalhes
