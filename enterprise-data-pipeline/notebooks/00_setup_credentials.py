# Databricks notebook source
# MAGIC %md
# MAGIC # 🔐 Credential Setup - Azure Key Vault
# MAGIC
# MAGIC **Purpose:** One-time setup to store Service Principal credentials in Azure Key Vault
# MAGIC
# MAGIC After running this notebook once, all other notebooks can retrieve credentials from Key Vault instead of hardcoding.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Add Secrets to Azure Key Vault
# MAGIC
# MAGIC Run these commands in your **local terminal** (not in Databricks):
# MAGIC
# MAGIC ```bash
# MAGIC # Login to Azure
# MAGIC az login
# MAGIC
# MAGIC # Add Service Principal credentials to Key Vault
# MAGIC az keyvault secret set \
# MAGIC   --vault-name kv-crypto-pipeline \
# MAGIC   --name azure-tenant-id \
# MAGIC   --value "518d08e5-ea11-4f47-bab2-dbaa4ebbbb76"
# MAGIC
# MAGIC az keyvault secret set \
# MAGIC   --vault-name kv-crypto-pipeline \
# MAGIC   --name azure-client-id \
# MAGIC   --value "6ef62d52-f175-4c59-b4fc-5b7c59e5384c"
# MAGIC
# MAGIC az keyvault secret set \
# MAGIC   --vault-name kv-crypto-pipeline \
# MAGIC   --name azure-client-secret \
# MAGIC   --value "YOUR_CLIENT_SECRET_HERE"
# MAGIC ```
# MAGIC
# MAGIC ⚠️ **Important:** Replace `YOUR_CLIENT_SECRET_HERE` with your actual client secret!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Test Retrieval from Key Vault
# MAGIC
# MAGIC After adding secrets to Key Vault, test if they can be retrieved:

# COMMAND ----------

import sys
import os

# Add src to path
sys.path.append("/Workspace/Users/ericmarques1999@gmail.com/data-engineer-portfolio/enterprise-data-pipeline/src")

# Bootstrap: Set initial credentials to access Key Vault for the first time
os.environ['AZURE_TENANT_ID'] = "518d08e5-ea11-4f47-bab2-dbaa4ebbbb76"
os.environ['AZURE_CLIENT_ID'] = "6ef62d52-f175-4c59-b4fc-5b7c59e5384c"
os.environ['AZURE_CLIENT_SECRET'] = "9e951b28-962c-4818-bfe7-396b5cb156c0"  # Replace with your secret

print("🔐 Bootstrap credentials configured")

# COMMAND ----------

from utils.config_loader import get_azure_service_principal_from_keyvault, setup_azure_credentials_from_keyvault

# Test: Retrieve Service Principal credentials from Key Vault
try:
    print("📥 Retrieving Service Principal credentials from Key Vault...")
    credentials = get_azure_service_principal_from_keyvault("kv-crypto-pipeline")
    
    print("✅ Successfully retrieved credentials!")
    print(f"   Tenant ID: {credentials['tenant_id'][:8]}...")
    print(f"   Client ID: {credentials['client_id'][:8]}...")
    print(f"   Client Secret: {'*' * 20}")
    
except Exception as e:
    print(f"❌ Error: {e}")
    print("\n💡 Solutions:")
    print("   1. Make sure you ran the az keyvault secret set commands above")
    print("   2. Verify Service Principal has 'Get' permission on Key Vault")
    print("   3. Check that bootstrap credentials are correct")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Setup Environment Variables from Key Vault
# MAGIC
# MAGIC After confirming retrieval works, you can setup environment variables automatically:

# COMMAND ----------

# Setup environment variables from Key Vault
# This will replace the hardcoded values with values from Key Vault
setup_azure_credentials_from_keyvault("kv-crypto-pipeline")

# Verify
print("\n🔍 Verifying environment variables:")
print(f"   AZURE_TENANT_ID: {os.getenv('AZURE_TENANT_ID')[:8]}...")
print(f"   AZURE_CLIENT_ID: {os.getenv('AZURE_CLIENT_ID')[:8]}...")
print(f"   AZURE_CLIENT_SECRET: {'*' * 20}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Next Steps
# MAGIC
# MAGIC Now that credentials are in Key Vault:
# MAGIC
# MAGIC 1. **Remove hardcoded secrets** from other notebooks
# MAGIC 2. **Use `setup_azure_credentials_from_keyvault()`** at the beginning of each notebook
# MAGIC 3. **Add `.gitignore`** to ensure secrets never go to Git
# MAGIC
# MAGIC ### Updated Notebook Pattern:
# MAGIC
# MAGIC ```python
# MAGIC # Bootstrap (minimal hardcoded credentials)
# MAGIC os.environ['AZURE_TENANT_ID'] = "your-tenant-id"
# MAGIC os.environ['AZURE_CLIENT_ID'] = "your-client-id"  
# MAGIC os.environ['AZURE_CLIENT_SECRET'] = "your-secret"
# MAGIC
# MAGIC # Load rest from Key Vault
# MAGIC from utils.config_loader import setup_azure_credentials_from_keyvault
# MAGIC setup_azure_credentials_from_keyvault("kv-crypto-pipeline")
# MAGIC
# MAGIC # Now all Key Vault access uses credentials from Key Vault itself!
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔐 Security Best Practices
# MAGIC
# MAGIC ✅ **DO:**
# MAGIC - Store all secrets in Azure Key Vault
# MAGIC - Use Service Principal with least privilege
# MAGIC - Rotate secrets regularly (every 3-6 months)
# MAGIC - Use Databricks Secrets for Databricks-specific secrets
# MAGIC - Add sensitive files to .gitignore
# MAGIC
# MAGIC ❌ **DON'T:**
# MAGIC - Commit secrets to Git
# MAGIC - Share secrets via Slack/Email
# MAGIC - Use same secrets across environments
# MAGIC - Give excessive Key Vault permissions
