# Azure Key Vault Setup para Credenciais Snowflake

## Visão Geral

Este projeto usa **Azure Key Vault** para armazenar credenciais do Snowflake de forma segura, substituindo o arquivo `.env` local. Isso é a melhor prática para produção e ambientes em nuvem.

## Pré-requisitos

1. **Conta Azure** com acesso a criar Key Vaults
2. **Azure CLI** instalado e configurado
3. **Permissões**:
   - `Microsoft.KeyVault/vaults/write` - Para criar o vault
   - `Microsoft.KeyVault/vaults/secrets/write` - Para adicionar secrets

## Passo 1: Instalar Azure SDK

```bash
pip install azure-identity azure-keyvault-secrets
```

Ou atualize o requirements.txt e instale:

```bash
pip install -r requirements.txt
```

## Passo 2: Criar Key Vault no Azure

### Opção A: Usando Azure Portal

1. Abra [portal.azure.com](https://portal.azure.com)
2. Clique em **+ Create a resource**
3. Procure por **Key Vault**
4. Preencha:
   - **Name**: `crypto-pipeline-kv` (deve ser único)
   - **Region**: Mesma região do seu Databricks
   - **Pricing tier**: Standard
5. Na aba **Access Policies**:
   - Clique **+ Add Access Policy**
   - **Secret permissions**: Get, List, Set, Delete
   - **Select principal**: Seu usuário/service principal
6. Clique **Review + create**

### Opção B: Usando Azure CLI

```bash
# Login no Azure
az login

# Criar Key Vault
az keyvault create \
  --name crypto-pipeline-kv \
  --resource-group <seu-resource-group> \
  --location eastus

# Adicionar permissões para seu usuário
az keyvault set-policy \
  --name crypto-pipeline-kv \
  --object-id $(az ad signed-in-user show --query objectId -o tsv) \
  --secret-permissions get list set delete
```

## Passo 3: Adicionar Credenciais Snowflake ao Key Vault

### Opção A: Azure Portal

1. Vá para o Key Vault criado
2. Clique em **Secrets** (esquerda)
3. Clique em **+ Generate/Import**
4. Para cada secret, preencha:

**Secret 1: snowflake-account**
- Name: `snowflake-account`
- Value: `xy12345.us-east-1` (sem https://)
- Tags: `snowflake=true`

**Secret 2: snowflake-user**
- Name: `snowflake-user`
- Value: `seu_usuario`

**Secret 3: snowflake-password**
- Name: `snowflake-password`
- Value: `sua_senha_segura`

**Secret 4: snowflake-warehouse**
- Name: `snowflake-warehouse`
- Value: `COMPUTE_WH`

**Secret 5: snowflake-database**
- Name: `snowflake-database`
- Value: `CRYPTO_DB`

**Secret 6: snowflake-schema**
- Name: `snowflake-schema`
- Value: `BRONZE`

### Opção B: Azure CLI

```bash
# Definir variáveis
VAULT_NAME="crypto-pipeline-kv"

# Adicionar cada secret
az keyvault secret set --vault-name $VAULT_NAME --name snowflake-account --value "xy12345.us-east-1"
az keyvault secret set --vault-name $VAULT_NAME --name snowflake-user --value "seu_usuario"
az keyvault secret set --vault-name $VAULT_NAME --name snowflake-password --value "sua_senha_segura"
az keyvault secret set --vault-name $VAULT_NAME --name snowflake-warehouse --value "COMPUTE_WH"
az keyvault secret set --vault-name $VAULT_NAME --name snowflake-database --value "CRYPTO_DB"
az keyvault secret set --vault-name $VAULT_NAME --name snowflake-schema --value "BRONZE"
```

### Opção C: PowerShell

```powershell
$vaultName = "crypto-pipeline-kv"

# Adicionar secrets
$secrets = @{
    "snowflake-account" = "xy12345.us-east-1"
    "snowflake-user" = "seu_usuario"
    "snowflake-password" = "sua_senha_segura"
    "snowflake-warehouse" = "COMPUTE_WH"
    "snowflake-database" = "CRYPTO_DB"
    "snowflake-schema" = "BRONZE"
}

foreach ($secret in $secrets.GetEnumerator()) {
    az keyvault secret set --vault-name $vaultName --name $secret.Key --value $secret.Value
}
```

## Passo 4: Usar no Código Python

### No Extrator (CryptoExtractor)

```python
from src.utils.config_loader import get_snowflake_credentials_from_keyvault
from src.loaders.snowflake_loader import SnowflakeLoader

# Recuperar credenciais do Key Vault
snowflake_creds = get_snowflake_credentials_from_keyvault(vault_name="crypto-pipeline-kv")

# Usar no loader
loader = SnowflakeLoader(config=snowflake_creds)
```

### Nos Notebooks Databricks

```python
# Databricks Notebook Cell
from src.utils.config_loader import get_snowflake_credentials_from_keyvault

# Recuperar credenciais
vault_name = "crypto-pipeline-kv"
snowflake_config = get_snowflake_credentials_from_keyvault(vault_name)

# Usar para conexão
import snowflake.connector
conn = snowflake.connector.connect(
    account=snowflake_config['account'],
    user=snowflake_config['user'],
    password=snowflake_config['password'],
    warehouse=snowflake_config['warehouse'],
    database=snowflake_config['database'],
    schema=snowflake_config['schema']
)
```

## Passo 5: Configurar Autenticação no Databricks

### Para Databricks Community Edition (Local)

Use `DefaultAzureCredential` - ele tenta na ordem:
1. Variáveis de ambiente (`AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID`)
2. Managed Identity (se em VM/Container Azure)
3. Azure CLI login local (`az login`)

**Login com Azure CLI:**
```bash
az login
```

### Para Databricks Cluster (Production)

Crie uma Service Principal:

```bash
# Criar Service Principal
az ad sp create-for-rbac --name crypto-pipeline-sp

# Output incluirá:
# "appId": "xxxx-xxxx-xxxx"
# "password": "xxxx-xxxx-xxxx"
# "tenant": "xxxx-xxxx-xxxx"
```

Adicione permissões ao Key Vault:

```bash
az keyvault set-policy \
  --name crypto-pipeline-kv \
  --spn <appId-do-service-principal> \
  --secret-permissions get list
```

No Databricks, defina as variáveis de ambiente:

```python
# No notebook Databricks
import os
os.environ['AZURE_CLIENT_ID'] = dbutils.secrets.get(scope='azure', key='client-id')
os.environ['AZURE_CLIENT_SECRET'] = dbutils.secrets.get(scope='azure', key='client-secret')
os.environ['AZURE_TENANT_ID'] = dbutils.secrets.get(scope='azure', key='tenant-id')
```

## Passo 6: Testar Conexão

### Teste Local

```bash
# Verificar se pode acessar o Key Vault
python3 << 'EOF'
from src.utils.config_loader import get_snowflake_credentials_from_keyvault

try:
    creds = get_snowflake_credentials_from_keyvault("crypto-pipeline-kv")
    print("✅ Credenciais recuperadas com sucesso!")
    print(f"Account: {creds['account']}")
    print(f"User: {creds['user']}")
    print(f"Warehouse: {creds['warehouse']}")
except Exception as e:
    print(f"❌ Erro: {e}")
EOF
```

### Teste no Databricks Notebook

```python
# Databricks Notebook Cell
from src.utils.config_loader import get_snowflake_credentials_from_keyvault

try:
    creds = get_snowflake_credentials_from_keyvault("crypto-pipeline-kv")
    print("✅ Credenciais recuperadas com sucesso!")
    print(f"Account: {creds['account']}")
    
    # Tentar conectar
    import snowflake.connector
    conn = snowflake.connector.connect(
        account=creds['account'],
        user=creds['user'],
        password=creds['password'],
        warehouse=creds['warehouse'],
        database=creds['database'],
        schema=creds['schema']
    )
    print("✅ Conexão Snowflake bem-sucedida!")
    conn.close()
except Exception as e:
    print(f"❌ Erro: {e}")
```

## Troubleshooting

### Erro: "AZURE_AUTHORITY_HOST not set"

```bash
# Windows PowerShell
$env:AZURE_AUTHORITY_HOST = "https://login.microsoftonline.com"

# Linux/Mac
export AZURE_AUTHORITY_HOST="https://login.microsoftonline.com"
```

### Erro: "You do not have permissions to perform action"

Verifique as permissões do seu usuário no Key Vault:

```bash
az keyvault show --name crypto-pipeline-kv \
  --query 'properties.accessPolicies' -o table
```

Adicione permissões se necessário:

```bash
az keyvault set-policy \
  --name crypto-pipeline-kv \
  --object-id $(az ad signed-in-user show --query objectId -o tsv) \
  --secret-permissions get list set delete
```

### Erro: "Key Vault not found"

Verifique o nome do vault:

```bash
az keyvault list --query "[].name" -o table
```

### Erro: "DefaultAzureCredential failed to authenticate"

Certifique-se que você fez login:

```bash
# Verificar login
az account show

# Se necessário, fazer login
az login
```

## Segurança

### ✅ Boas Práticas Implementadas

1. **Sem credenciais no código** - Tudo vem do Key Vault
2. **Sem arquivo .env** - Removido do repositório
3. **Permissões granulares** - Service Principal com acesso apenas a GET/LIST
4. **Auditoria** - Azure Key Vault registra todos os acessos
5. **Rotação de secrets** - Suporte nativo no Azure

### Rotação de Secrets

Quando precisar atualizar uma credencial:

```bash
# Atualizar secret
az keyvault secret set --vault-name crypto-pipeline-kv \
  --name snowflake-password --value "nova_senha_segura"

# Não precisa reiniciar a aplicação - DefaultAzureCredential busca novos valores
```

## Próximos Passos

1. ✅ Remover arquivo `.env` do repositório
2. ✅ Adicionar `.env` ao `.gitignore` (já deve estar)
3. ✅ Testar localmente com `az login`
4. ✅ Criar Service Principal para Databricks
5. ✅ Testar nos notebooks Databricks
6. ✅ Implementar rotação automática de secrets

## Referências

- [Azure Key Vault Documentation](https://learn.microsoft.com/en-us/azure/key-vault/)
- [Azure Identity Python SDK](https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/identity/azure-identity)
- [Azure Key Vault Secrets Python SDK](https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/keyvault/azure-keyvault-secrets)
- [Databricks Azure Integration](https://docs.databricks.com/dev-tools/auth.html)
