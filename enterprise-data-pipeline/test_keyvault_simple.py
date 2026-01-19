#!/usr/bin/env python3
"""
Script simples para testar conexão com Azure Key Vault
"""

def test_keyvault():
    print("🔐 Testando Azure Key Vault...\n")
    
    try:
        # Importar bibliotecas
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient
        
        # Configurar Key Vault
        vault_name = "kv-crypto-pipeline"
        vault_url = f"https://{vault_name}.vault.azure.net/"
        
        print(f"📍 Conectando em: {vault_url}")
        
        # Autenticar
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=vault_url, credential=credential)
        
        # Tentar recuperar um secret
        print("🔍 Recuperando secret: snowflake-account")
        secret = client.get_secret("snowflake-account")
        
        print(f"✅ SUCESSO! Secret recuperado: {secret.value}\n")
        
        # Listar todos os secrets Snowflake
        print("📋 Verificando todos os secrets Snowflake:")
        snowflake_secrets = [
            'snowflake-account',
            'snowflake-user',
            'snowflake-password',
            'snowflake-warehouse',
            'snowflake-database',
            'snowflake-schema'
        ]
        
        for secret_name in snowflake_secrets:
            try:
                secret = client.get_secret(secret_name)
                if secret_name == 'snowflake-password':
                    print(f"   ✅ {secret_name}: {'*' * 8}")
                else:
                    print(f"   ✅ {secret_name}: {secret.value}")
            except Exception as e:
                print(f"   ❌ {secret_name}: {str(e)}")
        
        print("\n🎉 Conexão com Azure Key Vault funcionando!")
        return True
        
    except Exception as e:
        print(f"\n❌ ERRO: {str(e)}\n")
        print("💡 Soluções:")
        print("   1. Instalar SDK: pip install azure-identity azure-keyvault-secrets")
        print("   2. Fazer login: az login")
        print("   3. Verificar nome do Key Vault: 'kv-crypto-pipeline'")
        print("   4. Verificar permissões no Azure Portal")
        return False


if __name__ == "__main__":
    test_keyvault()
