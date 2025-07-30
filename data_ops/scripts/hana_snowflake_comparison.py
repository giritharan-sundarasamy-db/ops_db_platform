import os
import sys
# Add the project root directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
import json
import argparse
from hdbcli import dbapi  # For SAP HANA
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from data_ops.methods.constants import Constants as constants
from data_ops.db_connect.db_connection import TargetSingleton, SourceSingleton

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

class SourceConnector(DatabaseConnector, SourceSingleton):
    """Handles connections and queries for Source Database."""
    
    def __init__(self, config=None):
        # Use an empty dict if no config is provided
        super().__init__(config or {})
        self._constants = constants()
        
    def connect(self):
        try:
            self.connection = SourceSingleton.get_connection()
            if self.connection:
                logger.info(f"Successfully connected to {self._constants.SOURCE_SYSTEM} database")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to connect to {self._constants.SOURCE_SYSTEM} database: {str(e)}")
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
            logger.error(f"Error executing {self._constants.SOURCE_SYSTEM} query: {str(e)}")
            raise

class TargetConnector(DatabaseConnector, TargetSingleton):
    """Handles connections and queries for Target Database."""

    def __init__(self, config=None):
        # Use an empty dict if no config is provided
        super().__init__(config or {})
        self._constants = constants()
        
    def connect(self):
        try:
            self.connection = TargetSingleton.get_connection()
            if self.connection:
                logger.info(f"Successfully connected to {self._constants.TARGET_SYSTEM} database")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to connect to {self._constants.TARGET_SYSTEM} database: {str(e)}")
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
            logger.error(f"Error executing {self._constants.TARGET_SYSTEM} query: {str(e)}")
            raise




class DatabaseComparator:
    """Compares data between source and target databases"""
    def __init__(self):
        self._constants = constants()
        self.src_sys = SourceConnector()
        self.tgt_sys = TargetConnector()
    
    def compare_tables(
        self,
        src_query: str,
        tgt_query: str,
        key_columns: List[str],
        compare_columns: Optional[List[str]] = None,
        chunk_size: int = 10000
    ) -> Dict[str, Any]:
        """
        Compare data between SAP HANA and Snowflake tables or Data models.
        
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
        logger.info(f"Fetching data from source system {self._constants.SOURCE_SYSTEM} ...")
        src_df = self.src_sys.execute_query(src_query)
        logger.info(f"Fetched {len(src_df)} rows from source system {self._constants.SOURCE_SYSTEM}")
        
        logger.info(f"Fetching data from target system {self._constants.TARGET_SYSTEM}...")
        tgt_df = self.tgt_sys.execute_query(tgt_query)
        logger.info(f"Fetched {len(tgt_df)} rows from target system {self._constants.TARGET_SYSTEM}")
        
        # Set index for comparison
        src_df = src_df.set_index(key_columns)
        tgt_df = tgt_df.set_index(key_columns)
        
        # Determine which columns to compare
        if compare_columns is None:
            # Get common columns excluding key columns
            compare_columns = list(set(src_df.columns) & set(tgt_df.columns))
        
        # Initialize result dictionary
        result = {
            'timestamp': datetime.now().isoformat(),
            'src_row_count': len(src_df),
            'tgt_row_count': len(tgt_df),
            'columns_compared': compare_columns,
            'only_in_src': [],
            'only_in_tgt': [],
            'value_differences': []
        }
        
        # Find rows only in Source
        only_in_src = src_df.index.difference(tgt_df.index)
        if not only_in_src.empty:
            result['only_in_src'] = src_df.loc[only_in_src].reset_index().to_dict('records')
        
        # Find rows only in Target
        only_in_tgt = tgt_df.index.difference(src_df.index)
        if not only_in_tgt.empty:
            result['only_in_tgt'] = tgt_df.loc[only_in_tgt].reset_index().to_dict('records')
        
        # Find common rows with different values
        common_keys = src_df.index.intersection(tgt_df.index)
        
        # Process in chunks to handle large datasets
        for i in range(0, len(common_keys), chunk_size):
            chunk_keys = common_keys[i:i + chunk_size]
            
            for key in chunk_keys:
                src_row = src_df.loc[key]
                tgt_row = tgt_df.loc[key]
                
                differences = {}
                for col in compare_columns:
                    src_val = src_row[col] if col in src_row else None
                    tgt_val = tgt_row[col] if col in tgt_row else None
                    
                    # Handle different data types (e.g., numpy types vs Python native)
                    if isinstance(src_val, (int, float)) and isinstance(tgt_val, (int, float)):
                        if abs(float(src_val) - float(tgt_val)) > 1e-10:  # Account for floating point precision
                            differences[col] = {
                                'src_value': src_val,
                                'tgt_value': tgt_val
                            }
                    elif src_val != tgt_val:
                        differences[col] = {
                            'src_value': src_val,
                            'tgt_value': tgt_val
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
        self.src_sys.close_connection()
        self.tgt_sys.close_connection()

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Compare data between source and target databases')
    
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
    # Parse command line arguments
    args = parse_arguments()
    
    # Initialize constants
    consts = constants()
    
    # Define queries using constants
    src_query = f'SELECT * FROM {consts.source_schema_name}{consts.source_table_name}'
    tgt_query = f'SELECT * FROM {consts.target_schema_name}{consts.target_table_name}'
    
    # Get key columns from constants
    key_columns = consts.key_columns
    
    # Log the queries and key columns being used
    logger.info(f"Source query: {src_query}")
    logger.info(f"Target query: {tgt_query}")
    logger.info(f"Using key columns from constants: {key_columns}")
    
    if not key_columns:
        logger.error("No key columns specified in constants. Please set the 'key_columns' list.")
        return
    
    # Initialize comparator
    comparator = DatabaseComparator()
    
    try:
        # Run comparison
        result = comparator.compare_tables(
            src_query=src_query,
            tgt_query=tgt_query,
            key_columns=key_columns,
            compare_columns=args.compare_columns,
            chunk_size=args.chunk_size
        )
        
        # Save results to file
        with open(args.output, 'w') as f:
            json.dump(result, f, indent=2)
        
        # Print summary
        print("\n=== Comparison Summary ===")
        print(f"Rows in {consts.SOURCE_SYSTEM}: {result['src_row_count']}")
        print(f"Rows in {consts.TARGET_SYSTEM}: {result['tgt_row_count']}")
        print(f"Rows only in {consts.SOURCE_SYSTEM}: {len(result['only_in_src'])}")
        print(f"Rows only in {consts.TARGET_SYSTEM}: {len(result['only_in_tgt'])}")
        print(f"Rows with value differences: {len(result['value_differences'])}")
        print(f"\nDetailed report saved to: {args.output}")
        
    except Exception as e:
        logger.error(f"Comparison failed: {str(e)}")
    finally:
        # Ensure connections are closed
        comparator.close_connections()

if __name__ == "__main__":
    main()
