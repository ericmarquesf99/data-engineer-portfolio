# 🔐 Security Setup Guide

Complete guide for secure credential management in the Enterprise Data Pipeline.

---

## 🎯 Architecture Overview

```
Databricks Notebook
       ↓
  (1) Retrieve Service Principal credentials
       ↓
   Databricks Secrets (Recommended)
       ↓
   [Fallback: Azure Key Vault]
       ↓
  (2) Use Service Principal to access Azure Key Vault
       ↓
  (3) Retrieve Snowflake credentials
       ↓
  Azure Key Vault
       ↓
  (4) Connect to Snowflake
```

---

## ✅ Setup Steps

### **Step 1: Add Service Principal Credentials to Azure Key Vault**

```bash
# Login to Azure
az login

# Add Service Principal credentials
az keyvault secret set \
  --vault-name kv-crypto-pipeline \
  --name azure-tenant-id \
  --value "YOUR_TENANT_ID"

az keyvault secret set \
  --vault-name kv-crypto-pipeline \
  --name azure-client-id \
  --value "YOUR_CLIENT_ID"

az keyvault secret set \
  --vault-name kv-crypto-pipeline \
  --name azure-client-secret \
  --value "YOUR_CLIENT_SECRET"
```

### **Step 2: Add Snowflake Credentials to Azure Key Vault**

```bash
az keyvault secret set \
  --vault-name kv-crypto-pipeline \
  --name snowflake-account \
  --value "YOUR_SNOWFLAKE_ACCOUNT"

az keyvault secret set \
  --vault-name kv-crypto-pipeline \
  --name snowflake-user \
  --value "YOUR_SNOWFLAKE_USER"

az keyvault secret set \
  --vault-name kv-crypto-pipeline \
  --name snowflake-password \
  --value "YOUR_SNOWFLAKE_PASSWORD"
```

### **Step 3: Configure Databricks Secrets (Recommended)**

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure Databricks CLI (you'll need workspace URL and token)
databricks configure --token
# Enter:
#   - Databricks Host: https://community.cloud.databricks.com
#   - Token: (generate from User Settings → Access Tokens)

# Create secret scope
databricks secrets create-scope --scope azure-keyvault

# Add Service Principal credentials
databricks secrets put --scope azure-keyvault --key azure-tenant-id
# (Enter value in editor that opens)

databricks secrets put --scope azure-keyvault --key azure-client-id
# (Enter value)

databricks secrets put --scope azure-keyvault --key azure-client-secret
# (Enter value)
```

---

## 🔍 How Notebooks Retrieve Credentials

### **Automatic Process (With Databricks Secrets Configured)**

```python
# Notebook automatically tries Databricks Secrets first
os.environ['AZURE_TENANT_ID'] = dbutils.secrets.get(scope="azure-keyvault", key="azure-tenant-id")
os.environ['AZURE_CLIENT_ID'] = dbutils.secrets.get(scope="azure-keyvault", key="azure-client-id")
os.environ['AZURE_CLIENT_SECRET'] = dbutils.secrets.get(scope="azure-keyvault", key="azure-client-secret")

# ✅ No hardcoded values!
```

### **Fallback Process (If Databricks Secrets Not Configured)**

```python
# If Databricks Secrets fail, notebook falls back to Azure Key Vault
from utils.config_loader import setup_azure_credentials_from_keyvault

# Bootstrap with minimal hardcoded credentials (only for first Key Vault access)
os.environ['AZURE_TENANT_ID'] = "..."
os.environ['AZURE_CLIENT_ID'] = "..."
os.environ['AZURE_CLIENT_SECRET'] = "..."

# Replace with values from Key Vault
setup_azure_credentials_from_keyvault("kv-crypto-pipeline")

# ⚠️ Requires bootstrap hardcoded values
```

---

## 🔐 Security Best Practices

### ✅ **DO:**

- **Use Databricks Secrets** for production workloads
- **Rotate secrets** every 3-6 months
- **Use separate Service Principals** for dev/staging/prod
- **Grant least privilege** permissions (only "Get" and "List" on Key Vault)
- **Monitor access** via Azure Key Vault logs
- **Add `.gitignore`** for sensitive files:
  ```gitignore
  config/.env
  *.key
  *.pem
  secrets.yaml
  .databricks-connect
  ```

### ❌ **DON'T:**

- ❌ Commit secrets to Git
- ❌ Share secrets via email/Slack
- ❌ Use same credentials across environments
- ❌ Give excessive Key Vault permissions (avoid "All" permissions)
- ❌ Store secrets in plain text files
- ❌ Use root/admin accounts for pipeline execution

---

## 🧪 Testing Credential Setup

### **Test 1: Databricks Secrets**

```python
# Run in Databricks notebook
try:
    tenant_id = dbutils.secrets.get(scope="azure-keyvault", key="azure-tenant-id")
    print(f"✅ Databricks Secrets working! Tenant ID: {tenant_id[:8]}...")
except Exception as e:
    print(f"❌ Databricks Secrets error: {e}")
```

### **Test 2: Azure Key Vault Access**

```python
# Run notebook: 00_test_keyvault.py
# Should successfully retrieve all secrets
```

### **Test 3: Snowflake Connection**

```python
# Run notebook: 01_extraction.py
# Should successfully connect and write to Bronze layer
```

---

## 🚨 Troubleshooting

### **Error: "Secret does not exist"**

**Solution:**
- Verify secret name matches exactly (case-sensitive)
- Check Key Vault name is correct
- Ensure Service Principal has "Get" permission

### **Error: "Authentication failed"**

**Solution:**
- Verify Service Principal credentials are correct
- Check if client secret is expired (regenerate if needed)
- Ensure correct Tenant ID

### **Error: "Databricks Secrets scope not found"**

**Solution:**
```bash
# Create scope
databricks secrets create-scope --scope azure-keyvault

# Verify
databricks secrets list-scopes
```

### **Error: "Insufficient permissions"**

**Solution:**
```bash
# Grant Key Vault access
az role assignment create \
  --role "Key Vault Secrets User" \
  --assignee <service-principal-appId> \
  --scope /subscriptions/<sub-id>/resourceGroups/<rg-name>/providers/Microsoft.KeyVault/vaults/<vault-name>
```

---

## 📚 Additional Resources

- [Azure Key Vault Documentation](https://learn.microsoft.com/azure/key-vault/)
- [Databricks Secrets](https://docs.databricks.com/security/secrets/index.html)
- [Service Principal Best Practices](https://learn.microsoft.com/azure/active-directory/develop/howto-create-service-principal-portal)
- [Credential Rotation Guide](https://learn.microsoft.com/azure/key-vault/secrets/overview-storage-keys)

---

## 🎓 Certification & Compliance

This setup follows:
- ✅ **Azure Well-Architected Framework** (Security pillar)
- ✅ **GDPR** requirements for credential storage
- ✅ **SOC 2** compliance patterns
- ✅ **NIST Cybersecurity Framework** guidelines

Perfect for demonstrating **production-grade security** in your portfolio! 🚀
