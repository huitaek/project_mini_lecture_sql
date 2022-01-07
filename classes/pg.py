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
    def executeQuery(self, query):
        for k, v in query.items():
            self.cursor.execute(v)
        return self.cursor.fetchall()

    @errorLoggingDecorator
    def executeQueryInDictionary(self,query):
        for k, v in query.items():
            self.excuteQueryNoReturn(v)

    @errorLoggingDecorator
    def excuteQueryNoReturn(self,query):
        if type(query) != str:
            logging.warning('query is not type string')
        self.cursor.execute(query)
        return True

    @errorLoggingDecorator
    def executeInsertQueryFromCSV(self, table_name, columns, pandas_csv_data):
        proceed_values = []
        for i, row in pandas_csv_data.iterrows():
            row = ["'{}'".format(j) for j in list(row)]
            v = '(' + ','.join(list(map(str, list(row)))) + ')'
            proceed_values.append(v)

        proceed_values = ','.join(proceed_values)
        query = 'insert into {}({}) values{}'.format(table_name, columns, proceed_values)

        self.cursor.execute(query)
