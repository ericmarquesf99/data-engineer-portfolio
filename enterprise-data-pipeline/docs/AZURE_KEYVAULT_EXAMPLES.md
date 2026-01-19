# Exemplo: Usando Azure Key Vault nos Notebooks Databricks

## No Notebook de Extração (01_extraction.py)

```python
# Databricks Notebook: 01_extraction

from src.utils.config_loader import get_snowflake_credentials_from_keyvault
from src.utils.logging_config import StructuredLogger
from src.extractors.coingecko_extractor import CryptoExtractor
import json

# Configurar logging
logger = StructuredLogger("extraction").logger

try:
    # 1. Recuperar credenciais do Key Vault
    logger.info("Retrieving Snowflake credentials from Azure Key Vault...")
    snowflake_config = get_snowflake_credentials_from_keyvault(vault_name="crypto-pipeline-kv")
    logger.info("✅ Credenciais carregadas com sucesso")
    
    # 2. Configurar extrator de API
    config = {
        "api_config": {
            "base_url": "https://api.coingecko.com/api/v3",
            "coins": ["bitcoin", "ethereum", "cardano"],
            "retry_attempts": 3,
            "timeout": 10
        },
        "snowflake": snowflake_config,
        "storage": {
            "dbfs_path": "/mnt/bronze/crypto/raw"
        }
    }
    
    # 3. Executar extração
    extractor = CryptoExtractor(config)
    data = extractor.extract()
    
    logger.info(f"✅ Extração completa: {len(data)} registros")
    
    # 4. Salvar resultado (DBFS)
    df = spark.createDataFrame(data)
    df.write.mode("overwrite").json(config["storage"]["dbfs_path"])
    
except Exception as e:
    logger.error(f"Erro na extração: {str(e)}")
    raise
```

## No Notebook de Carregamento (03_loading.py)

```python
# Databricks Notebook: 03_loading

from src.utils.config_loader import get_snowflake_credentials_from_keyvault
from src.utils.logging_config import StructuredLogger
from src.loaders.snowflake_loader import SnowflakeLoader
import snowflake.connector

logger = StructuredLogger("loading").logger

try:
    # 1. Recuperar credenciais do Key Vault
    logger.info("Retrieving Snowflake credentials from Azure Key Vault...")
    snowflake_config = get_snowflake_credentials_from_keyvault(vault_name="crypto-pipeline-kv")
    logger.info("✅ Credenciais carregadas")
    
    # 2. Testar conexão
    logger.info("Testing Snowflake connection...")
    conn = snowflake.connector.connect(
        account=snowflake_config['account'],
        user=snowflake_config['user'],
        password=snowflake_config['password'],
        warehouse=snowflake_config['warehouse'],
        database=snowflake_config['database'],
        schema=snowflake_config['schema']
    )
    logger.info("✅ Conexão Snowflake estabelecida")
    conn.close()
    
    # 3. Configurar loader
    config = {
        "snowflake": snowflake_config,
        "transformation": {
            "enable_quality_checks": True,
            "enable_type2_scd": True
        }
    }
    
    loader = SnowflakeLoader(config)
    
    # 4. Carregar dados (exemplo)
    df = spark.read.json("/mnt/silver/crypto/transformed")
    
    result = loader.load_to_snowflake(
        dataframe=df,
        table_name="CRYPTO_PRICES",
        load_type="merge"
    )
    
    logger.info(f"✅ Carregamento completo: {result}")
    
except Exception as e:
    logger.error(f"Erro no carregamento: {str(e)}")
    raise
```

## Script de Teste Local

```python
# test_azure_keyvault.py

import sys
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from utils.config_loader import get_snowflake_credentials_from_keyvault, get_azure_keyvault_secrets

def test_keyvault_connection():
    """Teste básico de conexão com Key Vault"""
    print("🧪 Testando conexão com Azure Key Vault...")
    
    try:
        # 1. Teste simples - recuperar um secret
        print("\n1️⃣ Teste de conexão básica...")
        secrets = get_azure_keyvault_secrets(
            vault_name="crypto-pipeline-kv",
            keys=["snowflake-account"]
        )
        print(f"✅ Acesso ao Key Vault OK: {list(secrets.keys())}")
        
        # 2. Teste de credenciais completas
        print("\n2️⃣ Teste de credenciais Snowflake...")
        creds = get_snowflake_credentials_from_keyvault(
            vault_name="crypto-pipeline-kv"
        )
        print("✅ Credenciais recuperadas:")
        for key, value in creds.items():
            if key == 'password':
                print(f"   {key}: {'*' * 8}")
            else:
                print(f"   {key}: {value}")
        
        # 3. Teste de conexão Snowflake
        print("\n3️⃣ Teste de conexão Snowflake...")
        try:
            import snowflake.connector
            conn = snowflake.connector.connect(
                account=creds['account'],
                user=creds['user'],
                password=creds['password'],
                warehouse=creds['warehouse'],
                database=creds['database'],
                schema=creds['schema']
            )
            print("✅ Conexão Snowflake estabelecida!")
            
            # Teste simples de query
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            print(f"✅ Query teste: {result}")
            
            conn.close()
        except Exception as e:
            print(f"⚠️ Erro na conexão Snowflake: {e}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Erro: {e}")
        print("\n💡 Solução:")
        print("   1. Verifique se fez login: az login")
        print("   2. Verifique o nome do Key Vault: 'crypto-pipeline-kv'")
        print("   3. Verifique as permissões: az keyvault show --name crypto-pipeline-kv")
        return False

if __name__ == "__main__":
    success = test_keyvault_connection()
    sys.exit(0 if success else 1)
```

## Estrutura de Configuração Esperada

### Secrets no Azure Key Vault

```yaml
crypto-pipeline-kv:
  secrets:
    snowflake-account: "xy12345.us-east-1"
    snowflake-user: "data_engineer"
    snowflake-password: "SecurePassword123!"
    snowflake-warehouse: "COMPUTE_WH"
    snowflake-database: "CRYPTO_DB"
    snowflake-schema: "BRONZE"
    
    # Opcionais para Databricks
    databricks-token: "dapi123..."
    databricks-workspace-id: "123456789"
```

### Variáveis de Ambiente (para Service Principal)

```bash
AZURE_CLIENT_ID=xxxx-xxxx-xxxx-xxxx
AZURE_CLIENT_SECRET=your-secret-here
AZURE_TENANT_ID=xxxx-xxxx-xxxx-xxxx
```

## Checklist de Implementação

- [ ] Azure SDK instalado: `pip install azure-identity azure-keyvault-secrets`
- [ ] Key Vault criado no Azure
- [ ] Secrets adicionados ao Key Vault
- [ ] Autenticação configurada (az login ou Service Principal)
- [ ] Arquivo `.env` removido ou adicionado ao .gitignore
- [ ] Notebooks testados localmente
- [ ] Notebooks deployed no Databricks
- [ ] Service Principal criado para produção
- [ ] Databricks configurado com credenciais Azure

## Segurança - Coisas a NÃO fazer

❌ Não colocar credenciais no código  
❌ Não commitar arquivo `.env` no git  
❌ Não compartilhar secrets em mensagens  
❌ Não usar credenciais hardcoded em notebooks  
❌ Não compartilhar access keys do Key Vault  

✅ Usar Azure Key Vault para produção  
✅ Usar Service Principal com permissões mínimas  
✅ Rotacionar secrets regularmente  
✅ Auditar acessos ao Key Vault  
✅ Usar roles e access policies no Azure
