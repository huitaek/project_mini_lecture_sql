import logging
from classes import pg
from controller.dataController import readCSVByPandas, preProcessingWeatherData
from lib import consts, queries
from lib.lib import errorLoggingDecorator
from controller.dataController import getCsvFileNames,insertFromCSVFuncForMatchSQLFormat
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib import font_manager, rc

logger = logging.getLogger()
logger.setLevel(logging.INFO)

@errorLoggingDecorator
def scenarioInsertToDbWeather(db,fileNames):
    for i in list(filter(lambda x:x.find('weather')>=0,fileNames)):
        data = readCSVByPandas(i, 'euc-kr')
        predData = preProcessingWeatherData(data)
        insertFromCSVFuncForMatchSQLFormat(db,'weather', consts.columns_weather_string, predData)

    return True

@errorLoggingDecorator
def scenarioInsertToDbAccident(db,fileNames):
    for i in list(filter(lambda x:x.find('accident')>=0,fileNames)):
        data = readCSVByPandas(i, 'utf-8')
        insertFromCSVFuncForMatchSQLFormat(db,'accident', consts.columns_accident_string, data)

    return True


@errorLoggingDecorator
def scenarioexecuteInsertCSVData(db, switch):
    fileNames = getCsvFileNames()
    if switch == 1: # Accident
        scenarioInsertToDbAccident(db, fileNames)
    elif switch == 2: # Weather
        scenarioInsertToDbWeather(db, fileNames)
    else:
        logging.warning('no matched switch number!!')
        return False

def scenarioDbTablesInit(db):
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
    # connect db    
    db = pg.DB()
    db.connectToDB(consts.dbInfo)

    # set db ----
    # scenarioDbTablesInit(db)
    
    # ---- Analyze
    
    # 한글폰트세팅
    font_path = "C:/Windows/Fonts/malgun.ttf"
    font = font_manager.FontProperties(fname=font_path).get_name()
    rc('font', family=font)
    
    # 도로유형에 따른 상해 및 사망자 정보
    res,cols = db.executeQueryHasReturn({'q':"""select
       rtl.road_form_l as "도로형태(대)",
       sum(c.death) as "총 사망자 수",
       sum(c.slight) as "총 중상자 수",
       sum(c.injured) as "총 부상자 수",
       sum(c.slight)+sum(c.wound) as "총 경상자 수"
from accident a
         join casualty c on c.accident_id = a.accident_id
         join road_type_l rtl on a.road_type_l_id = rtl.road_type_l_id
         join road_type rt on a.road_type_id = rt.road_type_id
        group by rtl.road_form_l;"""})
    df = pd.DataFrame(res,columns = cols)
    
    df.plot(kind='bar',x='도로형태(대)',y='총 사망자 수')
    plt.show()
    
    
    # Database Connection close
    db.closeDBConnection()

