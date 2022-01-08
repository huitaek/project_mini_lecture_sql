import time
from os import getcwd, listdir
from os.path import abspath, join, isfile
import pandas as pd

from lib import consts, lib
from lib.lib import errorLoggingDecorator


@errorLoggingDecorator
def readCSVByPandas(file, encoding):
    return pd.read_csv(file, encoding=encoding)  # utf-8 or euc-kr


@errorLoggingDecorator
def preProcessingWeatherData(data):
    values = []
    for i, row in data.iterrows():
        if pd.isna(row["rainfall"]):
            row["rainfall"] = 0
        if pd.isna(row["snow_drifts"]):
            row["snow_drifts"] = 0
        values.append(row)

    values = pd.DataFrame(values)
    return values


@errorLoggingDecorator
def getCsvFileNames():
    curPath = abspath(getcwd()) + "\\csvs"
    names = [join(curPath, l) for l in listdir(curPath) if isfile(join(curPath, l))]
    return list(filter(lambda x: ".csv" in x, names))

@errorLoggingDecorator
def InsertToDbWeatherData(db,fileNames):
    for i in list(filter(lambda x:x.find('weather')>=0,fileNames)):
        data = readCSVByPandas(i, 'euc-kr')
        predData = preProcessingWeatherData(data)
        insertFromCsv(db,'weather', consts.columns_weather_string, predData)

    return True

@errorLoggingDecorator
def InsertToDbAccidentData(db,fileNames):
    for i in list(filter(lambda x:x.find('accident')>=0,fileNames)):
        data = readCSVByPandas(i, 'utf-8')
        insertFromCsv(db,'accident', consts.columns_accident_string, data)

    return True

@errorLoggingDecorator
def makeCSVFileByTableName(db,tableName,query='select * from'):
    res,cols = db.executeQueryHasReturn({'q':' '.join([query, tableName])})
    df = pd.DataFrame(res,columns=cols)
    df.to_csv('\\'.join([lib.getCurPath(),"{}.csv".format(time.time_ns())]),encoding='utf-8')

@errorLoggingDecorator
def insertFromCsv(db, tableName, columns, dataFrame):
    proceed_values = []
    for i, row in dataFrame.iterrows():
        row = ["'{}'".format(j) for j in list(row)]
        v = "(" + ",".join(list(map(str, list(row)))) + ")"
        proceed_values.append(v)

    proceed_values = ",".join(proceed_values)
    query = {
        "q": "insert into {}({}) values{}".format(tableName, columns, proceed_values)
    }
    db.executeQueryNoReturn(query)
