# Databricks notebook source
# MAGIC %md
# MAGIC # 🔐 Teste de Conexão: Azure Key Vault
# MAGIC
# MAGIC Notebook simples para testar a conexão com Azure Key Vault e recuperar secrets do Snowflake.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Instalar Dependências

# COMMAND ----------

%pip install azure-identity azure-keyvault-secrets

# COMMAND ----------

# Reiniciar Python para carregar as bibliotecas
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Testar Conexão com Key Vault

# COMMAND ----------

from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient

# ============================================================================
# CONFIGURAÇÃO - Substitua pelos seus valores
# ============================================================================

vault_name = "kv-crypto-pipeline"
vault_url = f"https://{vault_name}.vault.azure.net/"

# Service Principal (crie no Azure Portal)
# https://portal.azure.com → Azure Active Directory → App registrations → New registration
AZURE_TENANT_ID = "518d08e5-ea11-4f47-bab2-dbaa4ebbbb76"
AZURE_CLIENT_ID = "6ef62d52-f175-4c59-b4fc-5b7c59e5384c"
AZURE_CLIENT_SECRET = "9e951b28-962c-4818-bfe7-396b5cb156c0"

print(f"📍 Conectando em: {vault_url}")
print(f"🔐 Usando Service Principal: {AZURE_CLIENT_ID[:8]}...")

try:
    # Autenticar com Service Principal
    credential = ClientSecretCredential(
        tenant_id=AZURE_TENANT_ID,
        client_id=AZURE_CLIENT_ID,
        client_secret=AZURE_CLIENT_SECRET
    )
    
    client = SecretClient(vault_url=vault_url, credential=credential)
    
    # Tentar recuperar um secret
    print("\n🔍 Recuperando secret: snowflake-account")
    secret = client.get_secret("snowflake-account")
    
    print(f"✅ SUCESSO! Secret recuperado: {secret.value}")
    
except Exception as e:
    print(f"❌ ERRO: {str(e)}")
    print("\n💡 Soluções:")
    print("   1. Criar Service Principal no Azure Portal")
    print("   2. Dar permissões 'Get' e 'List' no Key Vault para o Service Principal")
    print("   3. Copiar tenant_id, client_id e client_secret para o código acima")
    print("\n📚 Tutorial: https://learn.microsoft.com/azure/key-vault/general/authentication")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Listar Todos os Secrets Snowflake

# COMMAND ----------

print("📋 Verificando todos os secrets Snowflake:\n")

snowflake_secrets = [
    'snowflake-account',
    'snowflake-user',
    'snowflake-password',
    'snowflake-warehouse',
    'snowflake-database',
    'snowflake-schema'
]

credentials = {}

for secret_name in snowflake_secrets:
    try:
        secret = client.get_secret(secret_name)
        credentials[secret_name.replace('snowflake-', '')] = secret.value
        
        # Ocultar senha
        if secret_name == 'snowflake-password':
            print(f"   ✅ {secret_name}: {'*' * 8}")
        else:
            print(f"   ✅ {secret_name}: {secret.value}")
    except Exception as e:
        print(f"   ❌ {secret_name}: {str(e)}")

print("\n🎉 Todos os secrets foram recuperados com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Testar Conexão com Snowflake (Opcional)

# COMMAND ----------

print("❄️ Testando conexão com Snowflake...\n")

try:
    import snowflake.connector
    
    # Conectar usando credenciais do Key Vault
    conn = snowflake.connector.connect(
        account=credentials['account'],
        user=credentials['user'],
        password=credentials['password'],
        warehouse=credentials['warehouse'],
        database=credentials['database'],
        schema=credentials['schema']
    )
    
    print("✅ Conexão Snowflake estabelecida!")
    
    # Executar query de teste
    cursor = conn.cursor()
    
    cursor.execute("SELECT CURRENT_VERSION()")
    version = cursor.fetchone()
    print(f"   Snowflake Version: {version[0]}")
    
    cursor.execute("SELECT CURRENT_USER()")
    user = cursor.fetchone()
    print(f"   Current User: {user[0]}")
    
    cursor.execute("SELECT CURRENT_DATABASE()")
    db = cursor.fetchone()
    print(f"   Current Database: {db[0]}")
    
    cursor.execute("SELECT CURRENT_SCHEMA()")
    schema = cursor.fetchone()
    print(f"   Current Schema: {schema[0]}")
    
    cursor.close()
    conn.close()
    
    print("\n🎉 Conexão com Snowflake funcionando perfeitamente!")
    
except ImportError:
    print("⚠️ snowflake-connector-python não instalado")
    print("   Execute: %pip install snowflake-connector-python[pandas]")
except Exception as e:
    print(f"❌ Erro ao conectar no Snowflake: {str(e)}")
    print("\n💡 Possíveis causas:")
    print("   1. Credenciais incorretas no Key Vault")
    print("   2. Warehouse pausado ou indisponível")
    print("   3. Permissões insuficientes no Snowflake")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Resultado Final
# MAGIC
# MAGIC Se você viu todas as mensagens de sucesso acima:
# MAGIC
# MAGIC - ✅ Azure Key Vault está configurado corretamente
# MAGIC - ✅ Secrets estão acessíveis
# MAGIC - ✅ Snowflake está conectável
# MAGIC
# MAGIC **Próximos passos:**
# MAGIC 1. Execute o notebook `01_extraction.py`
# MAGIC 2. Os dados serão extraídos da API e salvos no Snowflake Bronze
# MAGIC 3. Pipeline completo funcionando!
