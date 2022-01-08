import logging
from classes import pg
from lib import consts, queries
from lib.lib import errorLoggingDecorator
from controller.dataController import InsertToDbAccidentData, InsertToDbWeatherData, getCsvFileNames
import controller.visualizationController as vc

logger = logging.getLogger()
logger.setLevel(logging.INFO)

@errorLoggingDecorator
def scenarioexecuteInsertCSVData(db, switch):
    fileNames = getCsvFileNames()
    if switch == 1: # Accident
        InsertToDbAccidentData(db, fileNames)
    elif switch == 2: # Weather
        InsertToDbWeatherData(db, fileNames)
    else:
        logging.warning('no matched switch number!!')
        return False

def scenarioSetDbTables(db):
    # Database connection
    
    # clean Database
    db.executeQueryNoReturn(queries.init_query)

    # create Tables
    db.executeQueryNoReturn(queries.create_tables)

    # insert Data To DB
    scenarioexecuteInsertCSVData(db,1)  
    scenarioexecuteInsertCSVData(db,2)  

    # insert, alter, update DB
    db.executeQueryNoReturn(queries.accident_daynight_update)
    db.executeQueryNoReturn(queries.involved_types_assaults_damageds_insert)
    db.executeQueryNoReturn(queries.location)
    db.executeQueryNoReturn(queries.casualty)
    db.executeQueryNoReturn(queries.violation)
    db.executeQueryNoReturn(queries.road_type_and_l)
    db.executeQueryNoReturn(queries.acc_type)
    db.executeQueryNoReturn(queries.extra_fk)
    
    return db



if __name__ == '__main__':
    # set db    
    db = pg.DB()
    db.connectToDB(consts.dbInfo)
    # scenarioSetDbTables(db)
    
    # Analyze and Visualizing
    # -- contents
    
    # ---- 도로형태(대분류)에 따른 상해 및 사망자 정보
    # vc.scenarioVisualizingRoadTypeL(db)
    
    # ---- 도로형태(소)에 따른 상해 및 사망자 정보
    # vc.scenarioVisualizingRoadType(db)
    
    
    
    # Database Connection close
    db.closeDBConnection()

