import logging
from classes import pg
from controller.dataController import readCSVByPandas, preProcessingWeatherData
from lib import consts, queries
from lib.lib import errorLoggingDecorator
from controller.dataController import getCsvFileNames

logger = logging.getLogger()
logger.setLevel(logging.INFO)

@errorLoggingDecorator
def scenarioInsertToDbWeather(db,fileNames):
    for i in list(filter(lambda x:x.find('weather')>=0,fileNames)):
        data = readCSVByPandas(i, 'euc-kr')
        predData = preProcessingWeatherData(data, consts.columns_weather)
        db.executeInsertQueryFromCSV('weather', consts.columns_weather_string, predData)

    return True


@errorLoggingDecorator
def scenarioInsertToDbAccident(db,fileNames):
    for i in list(filter(lambda x:x.find('accident')>=0,fileNames)):
        data = readCSVByPandas(i, 'utf-8')
        db.executeInsertQueryFromCSV('accident', consts.columns_accident_string, data)

    return True


@errorLoggingDecorator
def executeInsertScenario(db, switch):
    fileNames = getCsvFileNames()
    if switch == 1:
        scenarioInsertToDbAccident(db, fileNames)
    elif switch == 2:
        scenarioInsertToDbWeather(db, fileNames)
    else:
        logging.warning('no matched switch number!!')
        return False

if __name__ == '__main__':
    # Database connection
    db = pg.DB()
    db.connectToDB(consts.dbInfo)

    # clean Database
    db.excuteQueryNoReturn(queries.init_query)

    # create Tables
    db.executeQueryInDictionary(queries.create_tables)

    # insert Data To DB
    executeInsertScenario(db,1)  # Accident
    executeInsertScenario(db,2)  # Weather

    # insert, alter, update DB
    db.executeQueryInDictionary(queries.accident_daynight_update)
    db.executeQueryInDictionary(queries.involved_types_assaults_damageds_insert)
    db.executeQueryInDictionary(queries.location)
    db.executeQueryInDictionary(queries.casualty)
    db.executeQueryInDictionary(queries.violation)
    db.executeQueryInDictionary(queries.road_type_and_l)
    db.executeQueryInDictionary(queries.acc_type)
    db.executeQueryInDictionary(queries.extra_fk)


    # Database Connection close
    db.closeDBConnection()

    # -------------------------------


    # Analze


