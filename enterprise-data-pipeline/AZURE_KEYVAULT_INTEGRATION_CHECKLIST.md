# Checklist: Integração Azure Key Vault com Databricks

## ✅ O que já foi feito:

- ✓ Removido `python-dotenv` do requirements.txt
- ✓ Adicionado `azure-identity` e `azure-keyvault-secrets`
- ✓ Criada função `get_snowflake_credentials_from_keyvault()` em `config_loader.py`
- ✓ Criado script de teste: `test_azure_keyvault.py`
- ✓ Azure Key Vault `kv-crypto-pipeline` criado com secrets

## 🧪 Passo 1: Testar Localmente

Execute o script de teste:

```bash
cd c:\Users\ericm\repos\data-engineer-portfolio\enterprise-data-pipeline

# Login Azure
az login

# Instalar dependências (se necessário)
pip install -r requirements.txt

# Executar teste
python test_azure_keyvault.py
```

**Resultado esperado:**
```
✅ TODOS OS TESTES PASSARAM COM SUCESSO!
```

## 📝 Passo 2: Adaptar Notebooks para Azure Key Vault

### Notebook 01_extraction.py

Substituir:
```python
# ANTES (sem secrets)
config = {
    "api_config": {
        "base_url": "https://api.coingecko.com/api/v3",
        "coins": ["bitcoin", "ethereum", "cardano"]
    }
}
```

Por:
```python
# DEPOIS (com Azure Key Vault)
from src.utils.config_loader import get_snowflake_credentials_from_keyvault

snowflake_config = get_snowflake_credentials_from_keyvault(
    vault_name="kv-crypto-pipeline"
)

config = {
    "api_config": {
        "base_url": "https://api.coingecko.com/api/v3",
        "coins": ["bitcoin", "ethereum", "cardano"]
    },
    "snowflake": snowflake_config
}
```

### Notebook 03_loading.py

Substituir:
```python
# ANTES
snowflake_config = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    # ... mais hardcoded
}
```

Por:
```python
# DEPOIS
from src.utils.config_loader import get_snowflake_credentials_from_keyvault

snowflake_config = get_snowflake_credentials_from_keyvault(
    vault_name="kv-crypto-pipeline"
)
```

## 🚀 Passo 3: Deploy no Databricks

1. **Criar cluster Databricks**
   - Spark Runtime: 13.3 LTS+
   - Python: 3.11+
   - Workers: 2-4

2. **Instalar bibliotecas no cluster**
   ```
   snowflake-connector-python[pandas]==3.6.0
   pyyaml==6.0.1
   tenacity==8.2.3
   azure-identity==1.14.0
   azure-keyvault-secrets==4.7.0
   ```

3. **Upload dos notebooks**
   - `/notebooks/00_orchestrator.py`
   - `/notebooks/01_extraction.py`
   - `/notebooks/02_transformation.py`
   - `/notebooks/03_loading.py`

4. **Configurar autenticação Azure**
   
   No primeiro notebook, adicionar:
   ```python
   # Databricks Notebook Cell
   import os
   
   # Se usando Service Principal
   os.environ['AZURE_CLIENT_ID'] = dbutils.secrets.get(scope='azure', key='client-id')
   os.environ['AZURE_CLIENT_SECRET'] = dbutils.secrets.get(scope='azure', key='client-secret')
   os.environ['AZURE_TENANT_ID'] = dbutils.secrets.get(scope='azure', key='tenant-id')
   
   # Se usando Default Credentials (Managed Identity)
   # Nenhuma configuração necessária - apenas confie no cluster
   ```

## 🔧 Passo 4: Testar no Databricks

1. **Notebook de teste:**
   ```python
   # Databricks Notebook Cell
   from src.utils.config_loader import get_snowflake_credentials_from_keyvault
   
   try:
       creds = get_snowflake_credentials_from_keyvault("kv-crypto-pipeline")
       print("✅ Credenciais recuperadas!")
       print(f"Account: {creds['account']}")
   except Exception as e:
       print(f"❌ Erro: {e}")
   ```

2. **Testar extração:**
   - Executar `01_extraction.py`
   - Verificar se dados foram salvos em DBFS

3. **Testar transformação:**
   - Executar `02_transformation.py`
   - Verificar se transformações funcionam

4. **Testar carregamento:**
   - Executar `03_loading.py`
   - Verificar dados em Snowflake

## 📊 Passo 5: Criar Databricks Job

No Databricks:

1. **Workflows → Create job**
2. **Nome:** `crypto-pipeline-daily`
3. **Schedule:** Daily at 02:00 AM
4. **Tasks:**
   - Task 1: `00_orchestrator.py`
     - Depends on: None
   - Task 2: `01_extraction.py`
     - Depends on: Task 1
   - Task 3: `02_transformation.py`
     - Depends on: Task 2
   - Task 4: `03_loading.py`
     - Depends on: Task 3

## ⚠️ Troubleshooting

### Erro: "DefaultAzureCredential failed to authenticate"

**Solução:**
```bash
# Verificar login
az account show

# Se necessário, fazer login
az login
```

### Erro: "Key Vault not found"

**Solução:**
```bash
# Listar Key Vaults disponíveis
az keyvault list --query "[].name" -o table

# Verificar nome exato
```

### Erro: "You do not have permissions"

**Solução:**
```bash
# Verificar permissões
az keyvault show --name kv-crypto-pipeline --query 'properties.accessPolicies' -o table

# Adicionar permissões se necessário
az keyvault set-policy --name kv-crypto-pipeline \
  --object-id $(az ad signed-in-user show --query objectId -o tsv) \
  --secret-permissions get list set delete
```

### Erro: "Snowflake connection failed"

**Solução:**
```python
# Verificar credenciais
from src.utils.config_loader import get_azure_keyvault_secrets

secrets = get_azure_keyvault_secrets(
    vault_name="kv-crypto-pipeline",
    keys=["snowflake-account", "snowflake-user"]
)
print(secrets)
```

## 📋 Checklist Final

- [ ] Teste local passou (`python test_azure_keyvault.py`)
- [ ] Azure login configurado (`az login`)
- [ ] Notebooks adaptados para usar `get_snowflake_credentials_from_keyvault()`
- [ ] Dependências instaladas no Databricks cluster
- [ ] Autenticação Azure configurada no Databricks
- [ ] Notebooks fazer upload para Databricks
- [ ] Teste individual de cada notebook
- [ ] Databricks Job criado e testado
- [ ] Pipeline executando diariamente

## 🎯 Próximos Passos

1. **Executar teste local:** `python test_azure_keyvault.py`
2. **Reportar resultado** (sucesso ou erro)
3. **Adaptar notebooks** conforme necessário
4. **Deploy no Databricks**
