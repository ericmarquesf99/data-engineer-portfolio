#!/usr/bin/env python3
"""
Test Snowflake Connection
Validates that your Snowflake credentials are working
"""

import os
import sys
import logging
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load .env file
load_dotenv('../config/.env')

try:
    import snowflake.connector
    from snowflake.connector import DictCursor
except ImportError:
    logger.error("❌ snowflake-connector-python not installed")
    logger.info("Install with: pip install snowflake-connector-python")
    sys.exit(1)


def test_snowflake_connection():
    """Test Snowflake connection with your credentials"""
    
    # Get credentials from environment
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    user = os.getenv('SNOWFLAKE_USER')
    authenticator = os.getenv('SNOWFLAKE_AUTHENTICATOR', 'externalbrowser')
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
    database = os.getenv('SNOWFLAKE_DATABASE')
    schema = os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
    role = os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
    password = os.getenv('SNOWFLAKE_PASSWORD')
    
    # Validate credentials
    logger.info("🔍 Validating credentials...")
    if not account or not user:
        logger.error("❌ Missing SNOWFLAKE_ACCOUNT or SNOWFLAKE_USER in .env")
        return False
    
    logger.info(f"  Account: {account}")
    logger.info(f"  User: {user}")
    logger.info(f"  Authenticator: {authenticator}")
    logger.info(f"  Warehouse: {warehouse}")
    logger.info(f"  Database: {database}")
    logger.info(f"  Schema: {schema}")
    logger.info(f"  Role: {role}")
    
    # Build connection parameters
    conn_params = {
        'account': account,
        'user': user,
        'authenticator': authenticator,
        'warehouse': warehouse,
        'database': database,
        'schema': schema,
        'role': role,
    }
    
    # Add password only if not using browser auth
    if authenticator != 'externalbrowser' and password:
        conn_params['password'] = password
    
    # Test connection
    logger.info("\n🔗 Attempting connection to Snowflake...")
    
    try:
        conn = snowflake.connector.connect(**conn_params)
        cursor = conn.cursor(DictCursor)
        
        # Test query 1: Current user and warehouse
        logger.info("\n📊 Test 1: Checking user and warehouse...")
        cursor.execute("SELECT current_user() as user, current_warehouse() as warehouse, current_database() as database")
        result = cursor.fetchone()
        
        logger.info(f"  ✅ Connected as: {result['USER']}")
        logger.info(f"  ✅ Warehouse: {result['WAREHOUSE']}")
        logger.info(f"  ✅ Database: {result['DATABASE']}")
        
        # Test query 2: Snowflake version
        logger.info("\n📊 Test 2: Checking Snowflake version...")
        cursor.execute("SELECT current_version()")
        version = cursor.fetchone()
        logger.info(f"  ✅ Snowflake Version: {version[0]}")
        
        # Test query 3: List databases
        logger.info("\n📊 Test 3: Listing available databases...")
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()
        if databases:
            logger.info(f"  ✅ Found {len(databases)} databases:")
            for db in databases[:5]:  # Show first 5
                logger.info(f"     - {db['name']}")
            if len(databases) > 5:
                logger.info(f"     ... and {len(databases) - 5} more")
        else:
            logger.info("  ℹ️  No databases found")
        
        # Test query 4: Check if CRYPTO_DB exists
        logger.info("\n📊 Test 4: Checking for CRYPTO_DB...")
        cursor.execute("SELECT COUNT(*) as count FROM information_schema.databases WHERE database_name = 'CRYPTO_DB'")
        result = cursor.fetchone()
        if result['COUNT'] > 0:
            logger.info("  ✅ CRYPTO_DB exists")
        else:
            logger.info("  ⚠️  CRYPTO_DB not found (will be created when running pipeline)")
        
        # Test query 5: Check warehouse size
        logger.info("\n📊 Test 5: Warehouse information...")
        cursor.execute(f"DESC WAREHOUSE {warehouse}")
        warehouse_info = cursor.fetchone()
        if warehouse_info:
            logger.info(f"  ✅ Warehouse: {warehouse_info['name']}")
            logger.info(f"     Size: {warehouse_info['size']}")
            logger.info(f"     State: {warehouse_info.get('state', 'Unknown')}")
        
        cursor.close()
        conn.close()
        
        logger.info("\n" + "="*60)
        logger.info("✅ ALL TESTS PASSED!")
        logger.info("="*60)
        logger.info("\nYou can now run:")
        logger.info("  python pipeline_orchestrator.py")
        logger.info("\nOr set DATABASE_TYPE=snowflake:")
        logger.info("  export DATABASE_TYPE=snowflake")
        logger.info("  python pipeline_orchestrator.py")
        
        return True
        
    except Exception as e:
        logger.error(f"\n❌ Connection failed: {str(e)}")
        logger.info("\n🔧 Troubleshooting:")
        logger.info("  1. Check if SNOWFLAKE_ACCOUNT is correct (format: EYZZSXW-IR02741)")
        logger.info("  2. Verify SNOWFLAKE_USER is correct")
        logger.info("  3. If using password auth, check it's correct")
        logger.info("  4. If using externalbrowser, check if browser opened")
        logger.info("  5. Verify warehouse exists: SHOW WAREHOUSES in Snowflake")
        
        return False


if __name__ == "__main__":
    success = test_snowflake_connection()
    sys.exit(0 if success else 1)
