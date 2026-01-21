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

# MAGIC %pip install azure-identity azure-keyvault-secrets snowflake-connector-python[pandas] pyyaml tenacity requests
