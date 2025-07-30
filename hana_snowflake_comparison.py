import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
import json
import argparse
from hdbcli import dbapi  # For SAP HANA
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from constants import Constants
from db_connection import SnowflakeSingleton, HanaSingleton

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hana_snowflake_comparison.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseConnector:
    """Base class for database connections"""
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
    
    def connect(self):
        """Establish database connection"""
        raise NotImplementedError
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute a query and return results as a DataFrame"""
        raise NotImplementedError
    
    def close(self):
        """Close the database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None

class SourceConnector(DatabaseConnector, HanaSingleton):
    """Handles connections and queries for Source Database SAP HANA"""
    def __init__(self, config=None):
        # Use an empty dict if no config is provided
        super().__init__(config or {})
        
    def connect(self):
        try:
            self.connection = HanaSingleton.get_connection()
            if self.connection:
                logger.info("Successfully connected to SAP HANA")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to connect to SAP HANA: {str(e)}")
            raise
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        if not self.connection:
            self.connect()
            
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params or {})
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                return pd.DataFrame(data, columns=columns)
        except Exception as e:
            logger.error(f"Error executing HANA query: {str(e)}")
            raise

class TargetConnector(DatabaseConnector, SnowflakeSingleton):
    """Handles connections and queries for Target Database Snowflake"""
    def __init__(self, config=None):
        # Use an empty dict if no config is provided
        super().__init__(config or {})
        
    def connect(self):
        try:
            self.connection = SnowflakeSingleton.get_connection()
            if self.connection:
                logger.info("Successfully connected to Snowflake")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            raise
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        if not self.connection:
            self.connect()
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params or {})
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=columns)
        except Exception as e:
            logger.error(f"Error executing Snowflake query: {str(e)}")
            raise




class DatabaseComparator:
    """Compares data between SAP HANA and Snowflake databases"""
    def __init__(self):
        self.hana = SourceConnector()
        self.snowflake = TargetConnector()
    
    def compare_tables(
        self,
        hana_query: str,
        snowflake_query: str,
        key_columns: List[str],
        compare_columns: Optional[List[str]] = None,
        chunk_size: int = 10000
    ) -> Dict[str, Any]:
        """
        Compare data between SAP HANA and Snowflake tables
        
        Args:
            hana_query: SQL query for SAP HANA
            snowflake_query: SQL query for Snowflake
            key_columns: List of column names that form the primary key
            compare_columns: List of columns to compare (None means all columns)
            chunk_size: Number of rows to process at a time
            
        Returns:
            Dictionary with comparison results
        """
        logger.info("Starting table comparison...")
        
        # Execute queries
        logger.info("Fetching data from SAP HANA [Source system]...")
        hana_df = self.hana.execute_query(hana_query)
        logger.info(f"Fetched {len(hana_df)} rows from SAP HANA")
        
        logger.info("Fetching data from Snowflake [Target system]...")
        snowflake_df = self.snowflake.execute_query(snowflake_query)
        logger.info(f"Fetched {len(snowflake_df)} rows from Snowflake")
        
        # Set index for comparison
        hana_df = hana_df.set_index(key_columns)
        snowflake_df = snowflake_df.set_index(key_columns)
        
        # Determine which columns to compare
        if compare_columns is None:
            # Get common columns excluding key columns
            compare_columns = list(set(hana_df.columns) & set(snowflake_df.columns))
        
        # Initialize result dictionary
        result = {
            'timestamp': datetime.now().isoformat(),
            'hana_row_count': len(hana_df),
            'snowflake_row_count': len(snowflake_df),
            'columns_compared': compare_columns,
            'only_in_hana': [],
            'only_in_snowflake': [],
            'value_differences': []
        }
        
        # Find rows only in HANA
        only_in_hana = hana_df.index.difference(snowflake_df.index)
        if not only_in_hana.empty:
            result['only_in_hana'] = hana_df.loc[only_in_hana].reset_index().to_dict('records')
        
        # Find rows only in Snowflake
        only_in_snowflake = snowflake_df.index.difference(hana_df.index)
        if not only_in_snowflake.empty:
            result['only_in_snowflake'] = snowflake_df.loc[only_in_snowflake].reset_index().to_dict('records')
        
        # Find common rows with different values
        common_keys = hana_df.index.intersection(snowflake_df.index)
        
        # Process in chunks to handle large datasets
        for i in range(0, len(common_keys), chunk_size):
            chunk_keys = common_keys[i:i + chunk_size]
            
            for key in chunk_keys:
                hana_row = hana_df.loc[key]
                snowflake_row = snowflake_df.loc[key]
                
                differences = {}
                for col in compare_columns:
                    hana_val = hana_row[col] if col in hana_row else None
                    snowflake_val = snowflake_row[col] if col in snowflake_row else None
                    
                    # Handle different data types (e.g., numpy types vs Python native)
                    if isinstance(hana_val, (int, float)) and isinstance(snowflake_val, (int, float)):
                        if abs(float(hana_val) - float(snowflake_val)) > 1e-10:  # Account for floating point precision
                            differences[col] = {
                                'hana_value': hana_val,
                                'snowflake_value': snowflake_val
                            }
                    elif hana_val != snowflake_val:
                        differences[col] = {
                            'hana_value': hana_val,
                            'snowflake_value': snowflake_val
                        }
                
                if differences:
                    result['value_differences'].append({
                        'key': dict(zip(key_columns, key if isinstance(key, tuple) else [key])),
                        'differences': differences
                    })
        
        logger.info("Table comparison completed")
        return result
    
    def generate_report(self, comparison_result: Dict, output_file: str) -> None:
        """Generate a JSON report from the comparison results"""
        try:
            with open(output_file, 'w') as f:
                json.dump(comparison_result, f, indent=2, default=str)
            logger.info(f"Comparison report generated: {output_file}")
        except Exception as e:
            logger.error(f"Error generating report: {str(e)}")
            raise
    
    def close_connections(self):
        """Close all database connections"""
        self.hana.close()
        self.snowflake.close()

def parse_arguments():
    """Parse command line arguments"""
    # Create an instance of Constants to access the configuration
    constants = Constants()
    
    parser = argparse.ArgumentParser(description='Compare data between SAP HANA and Snowflake')
    
    # Comparison parameters
    comp_group = parser.add_argument_group('Comparison Parameters')
    comp_group.add_argument('--compare-columns', nargs='+', 
                          help='List of columns to compare (default: all common columns)')
    comp_group.add_argument('--output', default='hana_snowflake_comparison.json', 
                          help='Output file path (default: hana_snowflake_comparison.json)')
    comp_group.add_argument('--chunk-size', type=int, default=10000,
                          help='Number of rows to process at a time (default: 10000)')
    
    return parser.parse_args()
def main():
    """Main function"""
    # Initialize constants
    constants = Constants()
    
    # Parse command line arguments
    args = parse_arguments()
    
    # Define queries using constants
    hana_query = f'SELECT * FROM {constants.hana_schema_name}{constants.hana_table_name}'
    snowflake_query = f'SELECT * FROM {constants.hana_databse_name}{constants.sf_schema_name}{constants.sf_table_name}'
    
    # Get key columns from constants
    key_columns = constants.key_columns
    
    # Log the queries and key columns being used
    logger.info(f"HANA query: {hana_query}")
    logger.info(f"Snowflake query: {snowflake_query}")
    logger.info(f"Using key columns from constants: {key_columns}")
    
    if not key_columns:
        logger.warning("No key columns specified in constants. This may affect comparison accuracy.")
        logger.info("Will attempt to use all columns for comparison.")
    
    # Initialize comparator
    comparator = None
    try:
        comparator = DatabaseComparator()
        
        # Run comparison
        result = comparator.compare_tables(
            hana_query=hana_query,
            snowflake_query=snowflake_query,
            key_columns=key_columns,
            compare_columns=args.compare_columns,
            chunk_size=args.chunk_size
        )
        
        # Generate report
        comparator.generate_report(result, args.output)
        
        # Print summary
        print("\n=== Comparison Summary ===")
        print(f"Rows in SAP HANA: {result['hana_row_count']}")
        print(f"Rows in Snowflake: {result['snowflake_row_count']}")
        print(f"Rows only in SAP HANA: {len(result['only_in_hana'])}")
        print(f"Rows only in Snowflake: {len(result['only_in_snowflake'])}")
        print(f"Rows with value differences: {len(result['value_differences'])}")
        print(f"\nDetailed report saved to: {args.output}")
        
    except Exception as e:
        logger.error(f"Comparison failed: {str(e)}")
        raise
    finally:
        if comparator:
            comparator.close_connections()

if __name__ == "__main__":
    main()
