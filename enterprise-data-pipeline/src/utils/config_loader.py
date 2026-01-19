"""
Configuration Loader
====================

Load and manage multi-environment configurations.
"""

import yaml
import os
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv


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


def load_environment_variables(env_file: Optional[str] = None):
    """
    Load environment variables from .env file
    
    Args:
        env_file: Path to .env file (defaults to .env in project root)
    """
    if env_file is None:
        env_file = ".env"
    
    env_path = Path(env_file)
    if env_path.exists():
        load_dotenv(env_path)
    else:
        # Try parent directories
        for parent in [Path.cwd(), Path(__file__).parent.parent.parent]:
            env_path = parent / ".env"
            if env_path.exists():
                load_dotenv(env_path)
                break


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
