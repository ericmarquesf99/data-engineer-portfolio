#!/usr/bin/env python3
"""
Test Azure Key Vault Connection and Snowflake Integration
"""

import sys
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from utils.config_loader import get_azure_keyvault_secrets, get_snowflake_credentials_from_keyvault
from utils.logging_config import StructuredLogger

logger = StructuredLogger("test_keyvault").logger

def test_keyvault_connection():
    """Teste de conexão com Azure Key Vault"""
    print("\n" + "="*70)
    print("🧪 TESTE: Azure Key Vault Connection")
    print("="*70)
    
    try:
        # 1. Teste simples - recuperar um secret
        print("\n1️⃣ Testando conexão básica com Key Vault...")
        secrets = get_azure_keyvault_secrets(
            vault_name="kv-crypto-pipeline",
            keys=["snowflake-account"]
        )
        print(f"✅ Acesso ao Key Vault OK")
        print(f"   Secrets recuperados: {list(secrets.keys())}")
        
        logger.info("Successfully connected to Azure Key Vault", extra={
            "vault": "kv-crypto-pipeline",
            "secrets_count": len(secrets)
        })
        
        return True
        
    except Exception as e:
        print(f"\n❌ Erro na conexão: {str(e)}")
        logger.error(f"Failed to connect to Key Vault: {str(e)}")
        return False


def test_snowflake_credentials():
    """Teste de recuperação de credenciais Snowflake"""
    print("\n" + "="*70)
    print("🧪 TESTE: Snowflake Credentials from Key Vault")
    print("="*70)
    
    try:
        print("\n2️⃣ Recuperando credenciais Snowflake...")
        creds = get_snowflake_credentials_from_keyvault(
            vault_name="kv-crypto-pipeline"
        )
        
        print("✅ Credenciais recuperadas com sucesso:")
        for key, value in creds.items():
            if key == 'password':
                print(f"   {key}: {'*' * len(value)}")
            else:
                print(f"   {key}: {value}")
        
        logger.info("Successfully retrieved Snowflake credentials", extra={
            "account": creds.get('account'),
            "user": creds.get('user'),
            "warehouse": creds.get('warehouse'),
            "database": creds.get('database'),
            "schema": creds.get('schema')
        })
        
        return creds
        
    except Exception as e:
        print(f"\n❌ Erro ao recuperar credenciais: {str(e)}")
        logger.error(f"Failed to retrieve Snowflake credentials: {str(e)}")
        return None


def test_snowflake_connection(creds):
    """Teste de conexão com Snowflake"""
    print("\n" + "="*70)
    print("🧪 TESTE: Snowflake Connection")
    print("="*70)
    
    try:
        print("\n3️⃣ Testando conexão Snowflake...")
        
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
        
        # Query 1: Version
        print("\n   Executando query de teste...")
        cursor.execute("SELECT CURRENT_VERSION() as version")
        version = cursor.fetchone()
        print(f"   ✅ Snowflake Version: {version[0]}")
        
        # Query 2: Current user
        cursor.execute("SELECT CURRENT_USER() as user")
        user = cursor.fetchone()
        print(f"   ✅ Current User: {user[0]}")
        
        # Query 3: Current database
        cursor.execute("SELECT CURRENT_DATABASE() as database")
        db = cursor.fetchone()
        print(f"   ✅ Current Database: {db[0]}")
        
        # Query 4: Current schema
        cursor.execute("SELECT CURRENT_SCHEMA() as schema")
        schema = cursor.fetchone()
        print(f"   ✅ Current Schema: {schema[0]}")
        
        cursor.close()
        conn.close()
        
        logger.info("Snowflake connection test successful", extra={
            "version": version[0],
            "user": user[0],
            "database": db[0],
            "schema": schema[0]
        })
        
        return True
        
    except Exception as e:
        print(f"\n❌ Erro na conexão Snowflake: {str(e)}")
        logger.error(f"Failed to connect to Snowflake: {str(e)}", extra={
            "error": str(e)
        })
        print("\n💡 Possíveis soluções:")
        print("   1. Verifique as credenciais no Azure Key Vault")
        print("   2. Verifique se o usuário Snowflake está ativo")
        print("   3. Verifique se o warehouse está disponível")
        print("   4. Instale: pip install snowflake-connector-python[pandas]")
        return False


def main():
    """Executar todos os testes"""
    print("\n")
    print("╔" + "="*68 + "╗")
    print("║" + " "*15 + "🔐 Azure Key Vault & Snowflake Tests" + " "*17 + "║")
    print("╚" + "="*68 + "╝")
    
    # Teste 1: Conexão com Key Vault
    if not test_keyvault_connection():
        print("\n❌ Falha na conexão com Key Vault. Abortando testes.")
        print("\n💡 Solução:")
        print("   1. Verifique se fez login: az login")
        print("   2. Verifique o nome do Key Vault: 'kv-crypto-pipeline'")
        print("   3. Verifique as permissões")
        return False
    
    # Teste 2: Recuperar credenciais
    creds = test_snowflake_credentials()
    if not creds:
        print("\n❌ Falha ao recuperar credenciais. Abortando testes.")
        return False
    
    # Teste 3: Conexão com Snowflake
    if not test_snowflake_connection(creds):
        print("\n❌ Falha na conexão com Snowflake.")
        return False
    
    # Sucesso!
    print("\n" + "="*70)
    print("✅ TODOS OS TESTES PASSARAM COM SUCESSO!")
    print("="*70)
    print("\n🎉 Seu pipeline está pronto para:")
    print("   ✓ Recuperar credenciais do Azure Key Vault")
    print("   ✓ Conectar com Snowflake")
    print("   ✓ Executar nos notebooks Databricks")
    print("\n📝 Próximos passos:")
    print("   1. Deploy dos notebooks no Databricks")
    print("   2. Criar Databricks Job para orquestração")
    print("   3. Testar pipeline end-to-end")
    print("="*70 + "\n")
    
    return True


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️ Testes interrompidos pelo usuário")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Erro inesperado: {str(e)}")
        logger.error(f"Unexpected error during tests: {str(e)}")
        sys.exit(1)
