import os
import json
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Constants:
    ''' Source connection details HANA Database '''
    SOURCE_SYSTEM = os.getenv('SOURCE_SYSTEM', 'SAP HANA')
    SOURCE_HOST = os.getenv('SOURCE_HOST')
    SOURCE_PORT = int(os.getenv('SOURCE_PORT', '30015'))
    SOURCE_USER = os.getenv('SOURCE_USER')
    SOURCE_PASSWORD = os.getenv('SOURCE_PASSWORD')
    SOURCE_DATABASE = os.getenv('SOURCE_DATABASE')
    
    ''' Target connection details Snowflake Database '''
    TARGET_SYSTEM = os.getenv('TARGET_SYSTEM', 'SNOWFLAKE')
    TARGET_ACCOUNT = os.getenv('TARGET_ACCOUNT')
    TARGET_USER = os.getenv('TARGET_USER')
    TARGET_PASSWORD = os.getenv('TARGET_PASSWORD')
    TARGET_DATABASE = os.getenv('TARGET_DATABASE')
    TARGET_SCHEMA = os.getenv('TARGET_SCHEMA')
    TARGET_WAREHOUSE = os.getenv('TARGET_WAREHOUSE')
    
    ''' Mandatory Parameters '''
    source_database_name = os.getenv('SOURCE_DATABASE_NAME', '_SYS_BIC')
    source_table_name = os.getenv('SOURCE_TABLE_NAME', 'CV_201_SNAPSHOT')
    source_schema_name = os.getenv('SOURCE_SCHEMA_NAME', 'Businesslayer.snapshot/')
    target_table_name = os.getenv('TARGET_TABLE_NAME', 'CV_201_SNAPSHOT')
    target_schema_name = os.getenv('TARGET_SCHEMA_NAME', 'OPS_PUB.SNAPSHOT.')
    
    ''' Optional Parameters '''
    source_properties = os.getenv('SOURCE_PROPERTIES', "('PLACEHOLDER' = ('$$IP_SYSTEM_PREFIX$$', '*'))")
    
    # Key columns for comparison (primary key columns)
    key_columns = json.loads(os.getenv('KEY_COLUMNS', '["COLUMN1", "COLUMN2", "COLUMN3"]'))
    target_properties = os.getenv('TARGET_PROPERTIES', '')
    where_clause = os.getenv('WHERE_CLAUSE', '')
    column_to_exclude = os.getenv('COLUMN_TO_EXCLUDE', '')
