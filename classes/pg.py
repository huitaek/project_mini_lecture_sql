import psycopg2 as pg2
import logging
from lib.lib import error_Logging_decorator

class DB:
    def __init__(self):
        self._connection = False
        self._cursor = False

        logging.info('DB instance initialized')

    @error_Logging_decorator
    def connect_to_db(self, connectionInfo):
        if type(connectionInfo) != str:
            logging.warning('Connection Info is unavailable')
            assert Exception

        self._connection = pg2.connect(connectionInfo)
        self._connection.autocommit = True
        self._cursor = self._connection.cursor()

        logging.info('DB Connection SUCCESS!!')
        return True

    @error_Logging_decorator
    def close_db_connection(self):
        self._connection.close()
        logging.info('DB Connection Close SUCCESS!!')
        return True

    @error_Logging_decorator
    def execute_query_no_return(self, query):
        if type(query) != dict:
            logging.error('Query must be DICT !!')
            assert TypeError
            
        for k, v in query.items():
            self._cursor.execute(v)
    
    @error_Logging_decorator 
    def execute_query_has_return(self,query):
        if type(query) != dict:
            logging.error('Query must be DICT !!')
            assert TypeError
            
        for k, v in query.items():
            self._cursor.execute(v)
            
        column_names = [r[0] for r in self._cursor.description]
        return [self._cursor.fetchall(),column_names]
    
        