from typing import Any, Union
import psycopg2 as pg2
import logging
from lib.lib import error_Logging_decorator

class DB:
    def __init__(self):
        self._connection:Union[pg2.connection,bool] = False
        self._cursor:Union[pg2.cursor,bool] = False
        logging.info('DB instance initialized')

    @error_Logging_decorator
    def connect_to_db(self, connectionInfo:str) -> None:
        self._connection = pg2.connect(connectionInfo)
        self._connection.autocommit = True
        self._cursor = self._connection.cursor()

        logging.info('DB Connection SUCCESS!!')

    @error_Logging_decorator
    def close_db_connection(self) -> None:
        if isinstance(self._connection,pg2.connection) :
            self._connection.close()
            logging.info('DB Connection Close SUCCESS!!')
        else:
            raise pg2.DatabaseError

    @error_Logging_decorator
    def execute_query_no_return(self, query:dict[Any,str]) -> None:
        for k, v in query.items():
            self._cursor.execute(v)
    
    @error_Logging_decorator 
    def execute_query_has_return(self,query:dict[Any,str]) -> Union[list,None]:
        for k, v in query.items():
            self._cursor.execute(v)
            
        column_names = [r[0] for r in self._cursor.description]
        return [self._cursor.fetchall(),column_names]