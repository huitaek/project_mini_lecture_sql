import logging
from datetime import datetime
from os import getcwd
from os.path import abspath
import traceback
from typing import Any

def error_Logging_decorator(f:Any) -> Any:
    def Wrapper(*args,**kargs):
        try:
            return f(*args,**kargs)
        except Exception as e:
            logging.error(traceback.format_exc())
            return False

    return Wrapper


@error_Logging_decorator
def trans_to_date(date:str) -> datetime:
    #### 2020-00-00 00:00 ####

    # year and month
    year,month,day_time = date.split('-')

    # day and time
    day,time = day_time.split()

    # hour and min
    hour,min = time.split(':')

    t:list[int] = list(map(int,[year,month,day,hour,min]))
    return datetime(year=t[0],month=t[1],day=t[2],hour=t[3],minute=t[4])


@error_Logging_decorator
def get_cur_path() -> str:
    return abspath(getcwd())