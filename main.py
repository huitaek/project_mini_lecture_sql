import logging
from classes import pg
from controller.dataController import readCSVByPandas, preProcessingWeatherData
from lib import consts, queries
from lib.lib import errorLoggingDecorator
from controller.dataController import getCsvFileNames,executeInsertQueryFromCSV

logger = logging.getLogger()
logger.setLevel(logging.INFO)

@errorLoggingDecorator
def scenarioInsertToDbWeather(db,fileNames):
    for i in list(filter(lambda x:x.find('weather')>=0,fileNames)):
        data = readCSVByPandas(i, 'euc-kr')
        predData = preProcessingWeatherData(data, consts.columns_weather)
        executeInsertQueryFromCSV(db,'weather', consts.columns_weather_string, predData)

    return True

@errorLoggingDecorator
def scenarioInsertToDbAccident(db,fileNames):
    for i in list(filter(lambda x:x.find('accident')>=0,fileNames)):
        data = readCSVByPandas(i, 'utf-8')
        executeInsertQueryFromCSV(db,'accident', consts.columns_accident_string, data)

    return True


@errorLoggingDecorator
def executeInsertScenario(db, switch):
    fileNames = getCsvFileNames()
    if switch == 1: # Accident
        scenarioInsertToDbAccident(db, fileNames)
    elif switch == 2: # Weather
        scenarioInsertToDbWeather(db, fileNames)
    else:
        logging.warning('no matched switch number!!')
        return False

if __name__ == '__main__':
    
    # Database connection
    db = pg.DB()
    db.connectToDB(consts.dbInfo)

    # clean Database
    db.executeQuery(queries.init_query)

    # create Tables
    db.executeQuery(queries.create_tables)

    # insert Data To DB
    executeInsertScenario(db,1)  
    executeInsertScenario(db,2)  

    # insert, alter, update DB
    db.executeQuery(queries.accident_daynight_update)
    db.executeQuery(queries.involved_types_assaults_damageds_insert)
    db.executeQuery(queries.location)
    db.executeQuery(queries.casualty)
    db.executeQuery(queries.violation)
    db.executeQuery(queries.road_type_and_l)
    db.executeQuery(queries.acc_type)
    db.executeQuery(queries.extra_fk)


    # Database Connection close
    db.closeDBConnection()

    # -------------------------------


    # Analze


