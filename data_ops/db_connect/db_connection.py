import os
import snowflake.connector
from hdbcli import dbapi  # For SAP HANA
from data_ops.methods.constants import Constants

class SourceSingleton:
    __conn = None
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SourceSingleton, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized') or not self._initialized:
            self._constants = Constants()
            if SourceSingleton.__conn is None:
                self._connect()
            self._initialized = True

    def _connect(self):
        config = {
            'user': os.getenv("SOURCE_USER") or self._constants.SOURCE_USER,
            'password': os.getenv("SOURCE_PASSWORD") or self._constants.SOURCE_PASSWORD,
            'database': os.getenv("SOURCE_DATABASE") or self._constants.source_databse_name,
            'schema': os.getenv("SOURCE_SCHEMA") or self._constants.source_schema_name,
            'host': os.getenv("SOURCE_HOST") or self._constants.SOURCE_HOST,
            'port': int(os.getenv("SOURCE_PORT") or self._constants.SOURCE_PORT or 30015)
        }
        SourceSingleton.__conn = dbapi.connect(**config)

    @classmethod
    def get_connection(cls):
        if cls.__conn is None:
            consts = Constants()
            print(f'SourceSingleton is None. Instantiating a new {consts.SOURCE_SYSTEM} connection.')
            cls()
        return cls.__conn

    @classmethod
    def close_connection(cls):
        if cls.__conn is not None:
            try:
                cls.__conn.close()
            except Exception as e:
                print(f"Error closing {cls.src_sys} connection: {e}")
            finally:
                cls.__conn = None
    

class TargetSingleton:
    __conn = None
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TargetSingleton, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized') or not self._initialized:
            self._constants = Constants()
            if TargetSingleton.__conn is None:
                self._connect()
            self._initialized = True

    def _connect(self):
        config = {
            'user': os.getenv("TARGET_USER") or self._constants.TARGET_USER,
            'password': os.getenv("TARGET_PASSWORD") or self._constants.TARGET_PASSWORD,
            'account': os.getenv("TARGET_ACCOUNT") or self._constants.TARGET_ACCOUNT,
            'database': os.getenv("TARGET_DATABASE") or self._constants.TARGET_DATABASE,
            'schema': os.getenv("TARGET_SCHEMA") or self._constants.target_schema_name,
            'warehouse': os.getenv("TARGET_WAREHOUSE") or self._constants.TARGET_WAREHOUSE,
        }
        TargetSingleton.__conn = snowflake.connector.connect(**config)

    @classmethod
    def get_connection(cls):
        if cls.__conn is None:
            consts = Constants()
            print(f'TargetSingleton is None. Instantiating a new {consts.TARGET_SYSTEM} connection.')
            cls()
        return cls.__conn

    @classmethod
    def close_connection(cls):
        if cls.__conn is not None:
            try:
                cls.__conn.close()
            except Exception as e:
                print(f"Error closing {cls.tgt_sys} connection: {e}")
            finally:
                cls.__conn = None  