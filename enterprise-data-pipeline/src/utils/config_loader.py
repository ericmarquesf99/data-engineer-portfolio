"""
Configuration Loader
====================

Load and manage multi-environment configurations.
"""

import yaml
import os
from pathlib import Path
from typing import Dict, Any, Optional


class ConfigurationError(Exception):
    """Custom exception for configuration errors"""
    pass


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load configuration from YAML file
    
    Args:
        config_path: Path to config file (defaults to config/config.yaml)
        
    Returns:
        Configuration dictionary
        
    Raises:
        ConfigurationError: If config file not found or invalid
    """
    if config_path is None:
        # Try to find config in default locations
        possible_paths = [
            Path("config/config.yaml"),
            Path("../config/config.yaml"),
            Path(__file__).parent.parent.parent / "config" / "config.yaml"
        ]
        
        for path in possible_paths:
            if path.exists():
                config_path = str(path)
                break
        
        if config_path is None:
            raise ConfigurationError("Config file not found in default locations")
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config
    except FileNotFoundError:
        raise ConfigurationError(f"Config file not found: {config_path}")
    except yaml.YAMLError as e:
        raise ConfigurationError(f"Invalid YAML in config file: {e}")


def get_environment_config(
    environment: str = "development",
    base_config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Get environment-specific configuration
    
    Args:
        environment: Environment name (development, staging, production)
        base_config: Base configuration to merge with environment config
        
    Returns:
        Merged configuration dictionary
    """
    # Load base config if not provided
    if base_config is None:
        base_config = load_config()
    
    # Load environment-specific config
    env_config_path = Path("config/environments") / f"{environment}.yaml"
    
    if not env_config_path.exists():
        # Fall back to base config if environment file doesn't exist
        return base_config
    
    try:
        with open(env_config_path, 'r') as f:
            env_config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigurationError(f"Invalid environment config: {e}")
    
    # Deep merge configurations (environment overrides base)
    merged_config = _deep_merge(base_config, env_config)
    
    return merged_config


def get_azure_keyvault_secrets(vault_name: str, keys: list) -> Dict[str, str]:
    """
    Get secrets from Azure Key Vault
    
    Args:
        vault_name: Name of the Azure Key Vault (without .vault.azure.net)
        keys: List of secret names to retrieve
        
    Returns:
        Dictionary mapping secret names to their values
        
    Raises:
        ConfigurationError: If vault access fails or secret not found
        
    Environment Variables (for Service Principal authentication):
        AZURE_TENANT_ID: Azure AD tenant ID
        AZURE_CLIENT_ID: Service Principal client ID
        AZURE_CLIENT_SECRET: Service Principal client secret
    """
    try:
        from azure.identity import ClientSecretCredential, DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient
    except ImportError:
        raise ConfigurationError(
            "Azure SDK not installed. Install with: "
            "pip install azure-identity azure-keyvault-secrets"
        )
    
    vault_url = f"https://{vault_name}.vault.azure.net/"
    
    # Tentar autenticar via Service Principal (variáveis de ambiente)
    tenant_id = os.getenv('AZURE_TENANT_ID')
    client_id = os.getenv('AZURE_CLIENT_ID')
    client_secret = os.getenv('AZURE_CLIENT_SECRET')
    
    if tenant_id and client_id and client_secret:
        # Service Principal com credenciais explícitas
        credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )
    else:
        # Fallback para DefaultAzureCredential (Managed Identity, Azure CLI, etc)
        credential = DefaultAzureCredential()
    
    client = SecretClient(vault_url=vault_url, credential=credential)
    
    secrets = {}
    try:
        for key in keys:
            secret = client.get_secret(key)
            secrets[key] = secret.value
    except Exception as e:
        raise ConfigurationError(
            f"Failed to retrieve secrets from Azure Key Vault '{vault_name}': {e}"
        )
    
    return secrets


def get_databricks_secrets(scope: str, keys: list) -> Dict[str, str]:
    """
    Get secrets from Databricks Secret Scope
    
    Args:
        scope: Secret scope name
        keys: List of secret keys to retrieve
        
    Returns:
        Dictionary mapping keys to secret values
        
    Note:
        Only works in Databricks environment
    """
    try:
        from dbutils import get_dbutils
        dbutils = get_dbutils()
        
        secrets = {}
        for key in keys:
            secrets[key] = dbutils.secrets.get(scope=scope, key=key)
        
        return secrets
    except ImportError:
        raise ConfigurationError("dbutils not available - not in Databricks environment")


def get_azure_service_principal_from_keyvault(vault_name: str) -> Dict[str, str]:
    """
    Get Azure Service Principal credentials from Azure Key Vault
    
    This function uses a bootstrap approach: it retrieves the Service Principal
    credentials from Key Vault using hardcoded credentials initially, then
    sets them as environment variables for subsequent Key Vault access.
    
    Args:
        vault_name: Name of the Azure Key Vault (without .vault.azure.net)
        
    Returns:
        Dictionary with Service Principal credentials:
        {
            'tenant_id': str,
            'client_id': str,
            'client_secret': str
        }
        
    Note:
        This creates a circular dependency, so you must have initial credentials
        set via environment variables or use hardcoded values for first access.
    """
    secret_keys = [
        'azure-tenant-id',
        'azure-client-id',
        'azure-client-secret'
    ]
    
    secrets = get_azure_keyvault_secrets(vault_name, secret_keys)
    
    return {
        'tenant_id': secrets['azure-tenant-id'],
        'client_id': secrets['azure-client-id'],
        'client_secret': secrets['azure-client-secret']
    }


def setup_azure_credentials_from_keyvault(vault_name: str) -> None:
    """
    Setup Azure Service Principal credentials from Key Vault as environment variables
    
    This function retrieves Service Principal credentials from Key Vault and
    sets them as environment variables (AZURE_TENANT_ID, AZURE_CLIENT_ID, 
    AZURE_CLIENT_SECRET) for subsequent Key Vault access.
    
    Args:
        vault_name: Name of the Azure Key Vault (without .vault.azure.net)
        
    Note:
        Requires initial Service Principal credentials to be set via environment
        variables or hardcoded for the bootstrap process.
        
    Example:
        # Set initial credentials (one-time setup)
        os.environ['AZURE_TENANT_ID'] = 'your-tenant-id'
        os.environ['AZURE_CLIENT_ID'] = 'your-client-id'
        os.environ['AZURE_CLIENT_SECRET'] = 'your-client-secret'
        
        # Then setup from Key Vault for all subsequent calls
        setup_azure_credentials_from_keyvault('kv-crypto-pipeline')
    """
    credentials = get_azure_service_principal_from_keyvault(vault_name)
    
    os.environ['AZURE_TENANT_ID'] = credentials['tenant_id']
    os.environ['AZURE_CLIENT_ID'] = credentials['client_id']
    os.environ['AZURE_CLIENT_SECRET'] = credentials['client_secret']
    
    print(f"✅ Azure Service Principal credentials loaded from Key Vault: {vault_name}")


def get_snowflake_credentials_from_keyvault(
    vault_name: str,
    warehouse: str = 'SNOWFLAKE_LEARNING_WH',
    database: str = 'CRYPTO_DB',
    schema: str = 'PUBLIC'
) -> Dict[str, str]:
    """
    Convenience function to get Snowflake credentials from Azure Key Vault
    
    Args:
        vault_name: Name of the Azure Key Vault (without .vault.azure.net)
        warehouse: Snowflake warehouse name (default: SNOWFLAKE_LEARNING_WH)
        database: Snowflake database name (default: CRYPTO_DB)
        schema: Snowflake schema name (default: PUBLIC)
        
    Returns:
        Dictionary with Snowflake configuration:
        {
            'account': str,
            'user': str,
            'password': str,
            'warehouse': str,
            'database': str,
            'schema': str
        }
        
    Note:
        Retrieves only credentials from Azure Key Vault:
        - snowflake-account
        - snowflake-user
        - snowflake-password
        
        warehouse, database and schema are passed as parameters
    """
    secret_keys = [
        'snowflake-account',
        'snowflake-user',
        'snowflake-password'
    ]
    
    secrets = get_azure_keyvault_secrets(vault_name, secret_keys)
    
    return {
        'account': secrets['snowflake-account'],
        'user': secrets['snowflake-user'],
        'password': secrets['snowflake-password'],
        'warehouse': warehouse,
        'database': database,
        'schema': schema
    }


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deep merge two dictionaries
    
    Args:
        base: Base dictionary
        override: Override dictionary (takes precedence)
        
    Returns:
        Merged dictionary
    """
    result = base.copy()
    
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    
    return result


def validate_required_config(config: Dict[str, Any], required_keys: list) -> bool:
    """
    Validate that required configuration keys are present
    
    Args:
        config: Configuration dictionary
        required_keys: List of required keys (supports nested keys with dot notation)
        
    Returns:
        True if all required keys present
        
    Raises:
        ConfigurationError: If required keys are missing
    """
    missing_keys = []
    
    for key in required_keys:
        if '.' in key:
            # Handle nested keys
            keys = key.split('.')
            value = config
            for k in keys:
                if isinstance(value, dict) and k in value:
                    value = value[k]
                else:
                    missing_keys.append(key)
                    break
        else:
            if key not in config:
                missing_keys.append(key)
    
    if missing_keys:
        raise ConfigurationError(f"Missing required configuration keys: {', '.join(missing_keys)}")
    
    return True
