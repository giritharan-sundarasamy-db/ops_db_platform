import os
import snowflake.connector
from hdbcli import dbapi  # For SAP HANA
from constants import Constants

class SnowflakeSingleton:
    __conn = None
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SnowflakeSingleton, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized') or not self._initialized:
            self._constants = Constants()
            if SnowflakeSingleton.__conn is None:
                self._connect()
            self._initialized = True

    def _connect(self):
        config = {
            'user': os.getenv("SNOWFLAKE_USER") or self._constants.SNOWFLAKE_USER,
            'password': os.getenv("SNOWFLAKE_PASSWORD") or self._constants.SNOWFLAKE_PASSWORD,
            'account': os.getenv("SNOWFLAKE_ACCOUNT") or self._constants.SNOWFLAKE_ACCOUNT,
            'database': os.getenv("SNOWFLAKE_DATABASE") or self._constants.SNOWFLAKE_DATABASE,
            'schema': os.getenv("SNOWFLAKE_SCHEMA") or self._constants.SNOWFLAKE_SCHEMA,
            'warehouse': os.getenv("SNOWFLAKE_WAREHOUSE") or self._constants.SNOWFLAKE_WAREHOUSE,
        }
        SnowflakeSingleton.__conn = snowflake.connector.connect(**config)

    @classmethod
    def get_connection(cls):
        if cls.__conn is None:
            print('SnowflakeSingleton is None. Instantiating a new Snowflake connection.')
            cls()
        return cls.__conn

    @classmethod
    def close_connection(cls):
        if cls.__conn is not None:
            try:
                cls.__conn.close()
            except Exception as e:
                print(f"Error closing Snowflake connection: {e}")
            finally:
                cls.__conn = None
    

class HanaSingleton:
    __conn = None
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(HanaSingleton, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized') or not self._initialized:
            self._constants = Constants()
            if HanaSingleton.__conn is None:
                self._connect()
            self._initialized = True

    def _connect(self):
        config = {
            'address': os.getenv("HANA_HOST") or self._constants.HANA_HOST,
            'port': int(os.getenv("HANA_PORT") or self._constants.HANA_PORT),
            'user': os.getenv("HANA_USER") or self._constants.HANA_USER,
            'password': os.getenv("HANA_PASSWORD") or self._constants.HANA_PASSWORD,
            'databaseName': os.getenv("HANA_DATABASE") or self._constants.HANA_DATABASE,
        }
        HanaSingleton.__conn = dbapi.connect(**config)

    @classmethod
    def get_connection(cls):
        if cls.__conn is None:
            print('HanaSingleton is None. Instantiating a new HANA connection.')
            cls()
        return cls.__conn

    @classmethod
    def close_connection(cls):
        if cls.__conn is not None:
            try:
                cls.__conn.close()
            except Exception as e:
                print(f"Error closing HANA connection: {e}")
            finally:
                cls.__conn = None  