import logging
from classes.pg import DB
from controller.data_controller import get_csv_file_names, insert_to_db_accident_data, insert_to_db_weather_data
from lib import consts, queries
from lib.lib import error_Logging_decorator
import controller.visualization_controller as vc

logger = logging.getLogger()
logger.setLevel(logging.INFO)
#logging.basicConfig(filename='./filename.log', level=logging.ERROR)

@error_Logging_decorator
def scenario_execute_insert_csv_data(db:DB, switch:int):
    fileNames = get_csv_file_names()
    if switch == 1: # Accident
        insert_to_db_accident_data(db, fileNames)
    elif switch == 2: # Weather
        insert_to_db_weather_data(db, fileNames)
    else:
        logging.warning('no matched switch number!!')
        return False

@error_Logging_decorator
def scenario_set_db_tables(db:DB):
    # clean Database
    db.execute_query_no_return(queries.init_query)

    # create Tables
    db.execute_query_no_return(queries.create_tables)

    # insert Data To DB
    scenario_execute_insert_csv_data(db,1)  
    scenario_execute_insert_csv_data(db,2)  

    # insert, alter, update DB
    db.execute_query_no_return(queries.accident_daynight_update)
    db.execute_query_no_return(queries.involved_types_assaults_damageds_insert)
    db.execute_query_no_return(queries.location)
    db.execute_query_no_return(queries.casualty)
    db.execute_query_no_return(queries.violation)
    db.execute_query_no_return(queries.road_type_and_l)
    db.execute_query_no_return(queries.acc_type)
    db.execute_query_no_return(queries.extra_fk)
    
    return db



if __name__ == '__main__':
    # set db    
    db = DB()
    db.connect_to_db(consts.DB_INFO)
    
    scenario_set_db_tables(db)
    
    # Analyze and Visualizing
    # -- contents
    
    # ---- 도로형태(대분류)에 따른 상해 및 사망자 정보
    vc.scenario_visualizing_Road_type_L(db)
    
    # ---- 도로형태(소)에 따른 상해 및 사망자 정보
    vc.scenario_visualizing_road_type(db)
    
    
    
    # Database Connection close
    db.close_db_connection()

