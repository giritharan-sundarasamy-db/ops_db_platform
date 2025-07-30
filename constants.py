import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Constants:
    ''' Source connection details HANA Database '''
    
    HANA_HOST = os.getenv('HANA_HOST')
    HANA_PORT = os.getenv('HANA_PORT')
    HANA_USER = os.getenv('HANA_USER')
    HANA_PASSWORD = os.getenv('HANA_PASSWORD')
    HANA_DATABASE = os.getenv('HANA_DATABASE')
    
    ''' Target connection details Snowflake Database '''

    SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
    SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
    
    ''' Mandatory Parameters '''
    
    hana_table_name = 'CV_201_SNAPSHOT'
    hana_schema_name = 'Businesslayer.snapshot/'
    sf_table_name = 'CV_201_SNAPSHOT'
    sf_schema_name = 'OPS_PUB.SNAPSHOT.'

    ''' Optional Parameters '''
    
    hana_properties = "('PLACEHOLDER' = ('$$IP_SYSTEM_PREFIX$$', '*'))"
    
    # Key columns for comparison (primary key columns)
    key_columns = ["COLUMN1", "COLUMN2", "COLUMN3"]  # Replace with actual column names
    snowflake_properties = ""
    where_clause = "" #This should be added as per HANA database
    column_to_exclude = "" #Enclose each column with single quotes and separate with comma.
    hana_databse_name = "_SYS_BIC"
