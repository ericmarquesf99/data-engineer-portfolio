# Databricks notebook source
# MAGIC %md
# MAGIC # 📦 Setup: Instalação de Dependências
# MAGIC
# MAGIC Execute este notebook uma vez para instalar todas as dependências necessárias no cluster.
# MAGIC
# MAGIC **Dependências:**
# MAGIC - Azure Key Vault SDK
# MAGIC - Snowflake Connector
# MAGIC - Utilitários Python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Instalar Bibliotecas Python

# COMMAND ----------

# Instalar dependências principais
%pip install azure-identity==1.14.0 azure-keyvault-secrets==4.7.0 snowflake-connector-python[pandas]==3.6.0 pyyaml==6.0.1 tenacity==8.2.3 requests==2.31.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificar Instalação

# COMMAND ----------

print("✅ Verificando bibliotecas instaladas...\n")

try:
    import azure.identity
    print(f"✅ azure-identity: {azure.identity.__version__}")
except ImportError as e:
    print(f"❌ azure-identity: {e}")

try:
    import azure.keyvault.secrets
    print(f"✅ azure-keyvault-secrets: instalado")
except ImportError as e:
    print(f"❌ azure-keyvault-secrets: {e}")

try:
    import snowflake.connector
    print(f"✅ snowflake-connector-python: {snowflake.connector.__version__}")
except ImportError as e:
    print(f"❌ snowflake-connector-python: {e}")

try:
    import yaml
    print(f"✅ pyyaml: instalado")
except ImportError as e:
    print(f"❌ pyyaml: {e}")

try:
    import tenacity
    print(f"✅ tenacity: {tenacity.__version__}")
except ImportError as e:
    print(f"❌ tenacity: {e}")

try:
    import requests
    print(f"✅ requests: {requests.__version__}")
except ImportError as e:
    print(f"❌ requests: {e}")

print("\n🎉 Todas as dependências foram instaladas com sucesso!")
print("\n📝 Próximos passos:")
print("   1. Reinicie o kernel do Python (Detach & re-attach)")
print("   2. Execute o notebook 01_extraction.py")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚠️ IMPORTANTE
# MAGIC
# MAGIC Após instalar as bibliotecas:
# MAGIC 1. **Reinicie o kernel:** Menu → Detach & re-attach
# MAGIC 2. Ou reinicie o cluster inteiro se preferir
# MAGIC
# MAGIC As bibliotecas instaladas com `%pip` são temporárias (apenas para a sessão atual).
# MAGIC
# MAGIC **Para instalação permanente:**
# MAGIC - Vá em Compute → Seu Cluster → Libraries
# MAGIC - Clique em "Install New"
# MAGIC - Adicione cada biblioteca via PyPI

# COMMAND ----------

dbutils.notebook.exit("success")
