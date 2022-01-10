import time
from os import getcwd, listdir
from os.path import abspath, join, isfile
import pandas as pd
from classes.pg import DB
from lib import consts, lib
from lib.lib import error_Logging_decorator

# CSV 파일 데이터 읽어오는 함수
@error_Logging_decorator
def read_csv_by_pandas(file:str, encoding:str) -> pd.DataFrame:
    return pd.read_csv(file, encoding=encoding)  # utf-8 or euc-kr


# 날씨 데이터 null 값 전처리 함수
@error_Logging_decorator
def preprocessing_weather_data(data:pd.DataFrame) -> pd.DataFrame:
    values = []
    for i, row in data.iterrows():
        if pd.isna(row["rainfall"]):
            row["rainfall"] = 0
        if pd.isna(row["snow_drifts"]):
            row["snow_drifts"] = 0
        values.append(row)

    values = pd.DataFrame(values)
    return values


# csvs 폴더 내 파일 이름을 가져오는 함수
@error_Logging_decorator
def get_csv_file_names() -> list:
    cur_path = abspath(getcwd()) + "\\csvs"
    names = [join(cur_path, l) for l in listdir(cur_path) if isfile(join(cur_path, l))]
    return list(filter(lambda x: ".csv" in x, names))


# 날씨 데이터를 DB에 넣는 함수
@error_Logging_decorator
def insert_to_db_weather_data(db,file_names:str) -> bool:
    for i in list(filter(lambda x:x.find('weather')>=0,file_names)):
        data = read_csv_by_pandas(i, 'euc-kr')
        predData = preprocessing_weather_data(data)
        insert_from_csv(db,'weather',','.join(consts.COLUMNS_WEATHER), predData)

    return True


# 사고 데이터를 DB에 넣는 함수
@error_Logging_decorator
def insert_to_db_accident_data(db:DB,file_names:str) -> bool:
    for i in list(filter(lambda x:x.find('accident')>=0,file_names)):
        data = read_csv_by_pandas(i, 'utf-8')
        insert_from_csv(db,'accident', ','.join(consts.COLUMNS_ACCIDENT), data)

    return True


# 테이블 이름
@error_Logging_decorator
def make_csv_file_from_table_data(db:DB,tableName:str,query:str='select * from') -> bool:
    res,cols = db.execute_query_has_return({'q':' '.join([query, tableName])})
    df = pd.DataFrame(res,columns=cols)
    df.to_csv('\\'.join([lib.getCurPath(),"{}.csv".format(time.time_ns())]),encoding='utf-8')
    
    return True


# CSV로부터 읽어온 데이터를 삽입하는 함수
@error_Logging_decorator
def insert_from_csv(db:DB, table_name:str, columns:list, dataFrame:pd.DataFrame) -> bool:
    proceed_values = []
    for i, row in dataFrame.iterrows():
        row = ["'{}'".format(j) for j in list(row)]
        v = "(" + ",".join(list(map(str, list(row)))) + ")"
        proceed_values.append(v)

    proceed_values = ",".join(proceed_values)
    query = {
        "q": "insert into {}({}) values{}".format(table_name, columns, proceed_values)
    }
    db.execute_query_no_return(query)
    
    return True
