import logging
from datetime import datetime
from os import getcwd
from os.path import abspath
import traceback


def errorLoggingDecorator(f):
    def Wrapper(*args,**kargs):
        try:
            return f(*args,**kargs)
        except Exception as e:
            logging.error(traceback.format_exc())
            return False

    return Wrapper

@errorLoggingDecorator
def transToDate(date):
    #### 2020-00-00 00:00 ####

    # year and month
    year,month,day_time = date.split('-')

    # day and time
    day,time = day_time.split()

    # hour and min
    hour,min = time.split(':')

    return datetime(*list(map(int,[year,month,day,hour,min])))

@errorLoggingDecorator
def getCurPath():
    return abspath(getcwd())