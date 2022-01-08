import psycopg2 as pg2
import logging
from lib.lib import errorLoggingDecorator


class DB:
    def __init__(self):
        self.connection = False
        self.cursor = False

        logging.info('DB instance initialized')

    @errorLoggingDecorator
    def connectToDB(self, connectionInfo):
        if type(connectionInfo) != str:
            logging.warning('Connection Info is unavailable')
            assert Exception

        self.connection = pg2.connect(connectionInfo)
        self.connection.autocommit = True
        self.cursor = self.connection.cursor()

        logging.info('DB Connection SUCCESS!!')
        return True

    @errorLoggingDecorator
    def closeDBConnection(self):
        self.connection.close()
        logging.info('DB Connection Close SUCCESS!!')
        return True

    @errorLoggingDecorator
    def executeQueryNoReturn(self, query):
        if type(query) != dict:
            logging.error('Query must be DICT !!')
            assert TypeError
            
        for k, v in query.items():
            self.cursor.execute(v)
            
    def executeQueryHasReturn(self,query):
        if type(query) != dict:
            logging.error('Query must be DICT !!')
            assert TypeError
            
        for k, v in query.items():
            self.cursor.execute(v)
        columnNames = [r[0] for r in self.cursor.description]
        return [self.cursor.fetchall(),columnNames]
    
        