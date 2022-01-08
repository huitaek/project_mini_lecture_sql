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
def executePandasToCsvOfAllDataByTableName(db, table):
    res = db.executeQuery(join(["select * from ", table]))

    if table.find("weather") >= 0:
        cols = consts.columns_weather
        cols.insert(0, "weather_id")
        df = pd.DataFrame(res, columns=cols)
        df.to_csv(join(lib.getCurPath(), "weather.csv"), encoding="euc-kr")
    elif table.find("accident") >= 0:
        cols = consts.columns_accident
        cols.insert(0, "accident_id")
        df = pd.DataFrame(res, columns=cols)
        df.to_csv(join(lib.getCurPath(), "accident.csv"), encoding="utf-8")
    else:
        df = pd.DataFrame(res)
        df.to_csv(join(lib.getCurPath(), "else.csv"), encoding="utf-8")


@errorLoggingDecorator
def executeInsertQueryFromCSV(db, table_name, columns, dataFrame):
    proceed_values = []
    for i, row in dataFrame.iterrows():
        row = ["'{}'".format(j) for j in list(row)]
        v = "(" + ",".join(list(map(str, list(row)))) + ")"
        proceed_values.append(v)

    proceed_values = ",".join(proceed_values)
    query = {
        "q": "insert into {}({}) values{}".format(table_name, columns, proceed_values)
    }
    db.executeQuery(query)
