# Guia: Criar Service Principal para Azure Key Vault

## 📋 O que você precisa:

Para acessar o Azure Key Vault do Databricks Community Edition, você precisa criar um **Service Principal** (App Registration) no Azure.

## 🚀 Passo a Passo

### 1. Criar Service Principal no Azure Portal

1. Acesse: https://portal.azure.com
2. Vá em **Azure Active Directory** (ou **Microsoft Entra ID**)
3. No menu esquerdo: **App registrations**
4. Clique **+ New registration**
5. Preencha:
   - **Name**: `databricks-keyvault-access`
   - **Supported account types**: `Single tenant`
   - **Redirect URI**: Deixe em branco
6. Clique **Register**

### 2. Copiar IDs Necessários

Após criar, você verá a página do App. **Copie estes valores:**

```
Application (client) ID: 12345678-1234-1234-1234-123456789012
Directory (tenant) ID:   87654321-4321-4321-4321-210987654321
```

### 3. Criar Client Secret

1. No menu esquerdo, clique **Certificates & secrets**
2. Clique na aba **Client secrets**
3. Clique **+ New client secret**
4. Preencha:
   - **Description**: `databricks-access`
   - **Expires**: `180 days` (ou conforme sua política)
5. Clique **Add**
6. **⚠️ IMPORTANTE**: Copie o **Value** AGORA! Ele não será mostrado novamente.

```
Client Secret Value: abc123~DEF456-GHI789...
```

### 4. Dar Permissões no Key Vault

1. Vá para o seu Key Vault: `kv-crypto-pipeline`
2. No menu esquerdo: **Access policies**
3. Clique **+ Create**
4. **Permissions**:
   - Secret permissions: ✅ **Get**, ✅ **List**
   - Clique **Next**
5. **Principal**:
   - Pesquise por: `databricks-keyvault-access`
   - Selecione o App que você criou
   - Clique **Next**
6. **Review + create**: Clique **Create**

### 5. Configurar no Databricks

#### Opção A: Direto no Código (Teste rápido)

```python
# No notebook
AZURE_TENANT_ID = "87654321-4321-4321-4321-210987654321"
AZURE_CLIENT_ID = "12345678-1234-1234-1234-123456789012"
AZURE_CLIENT_SECRET = "abc123~DEF456..."
```

#### Opção B: Usando Databricks Secrets (Recomendado para produção)

**⚠️ Não disponível no Community Edition**

Se você tiver Databricks Standard/Premium:

```bash
# No seu terminal local
databricks secrets create-scope --scope azure-sp
databricks secrets put --scope azure-sp --key tenant-id --string-value "87654321-4321-4321-4321-210987654321"
databricks secrets put --scope azure-sp --key client-id --string-value "12345678-1234-1234-1234-123456789012"
databricks secrets put --scope azure-sp --key client-secret --string-value "abc123~DEF456..."
```

No notebook:
```python
import os
os.environ['AZURE_TENANT_ID'] = dbutils.secrets.get(scope='azure-sp', key='tenant-id')
os.environ['AZURE_CLIENT_ID'] = dbutils.secrets.get(scope='azure-sp', key='client-id')
os.environ['AZURE_CLIENT_SECRET'] = dbutils.secrets.get(scope='azure-sp', key='client-secret')
```

## 🧪 Testar Conexão

Execute o notebook `00_test_keyvault.py` com as credenciais:

```python
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient

credential = ClientSecretCredential(
    tenant_id="seu-tenant-id",
    client_id="seu-client-id",
    client_secret="seu-client-secret"
)

vault_url = "https://kv-crypto-pipeline.vault.azure.net/"
client = SecretClient(vault_url=vault_url, credential=credential)

# Testar
secret = client.get_secret("snowflake-account")
print(f"✅ Sucesso: {secret.value}")
```

## ✅ Checklist de Validação

- [ ] Service Principal criado no Azure AD
- [ ] Copiou Tenant ID
- [ ] Copiou Client ID
- [ ] Copiou Client Secret (Value)
- [ ] Adicionou permissões no Key Vault (Get + List)
- [ ] Testou no notebook `00_test_keyvault.py`
- [ ] Secrets Snowflake foram recuperados com sucesso

## 🔒 Segurança

**❌ NÃO FAÇA:**
- Commitar credenciais no Git
- Compartilhar Client Secret em mensagens/email
- Deixar hardcoded em código de produção

**✅ FAÇA:**
- Use Databricks Secrets (quando disponível)
- Rotacione Client Secrets a cada 90-180 dias
- Use permissões mínimas (Get + List apenas)
- Monitore acessos no Azure Monitor

## 📚 Referências

- [Azure Service Principal](https://learn.microsoft.com/azure/active-directory/develop/app-objects-and-service-principals)
- [Key Vault Authentication](https://learn.microsoft.com/azure/key-vault/general/authentication)
- [Azure Identity SDK](https://learn.microsoft.com/python/api/overview/azure/identity-readme)

## 💡 Troubleshooting

### Erro: "Forbidden"
- Verificar se as permissões foram adicionadas corretamente no Key Vault
- Aguardar 1-2 minutos para propagação das permissões

### Erro: "Authentication failed"
- Verificar se copiou os valores corretos
- Tenant ID e Client ID devem ser UUIDs
- Client Secret deve incluir o sufixo completo (após o ~)

### Erro: "Key Vault not found"
- Verificar nome do Key Vault: `kv-crypto-pipeline`
- Verificar se o Key Vault está na mesma subscription
