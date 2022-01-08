# class?

import pandas as pd
from matplotlib import pyplot as plt
from matplotlib import font_manager,rc

from lib.lib import errorLoggingDecorator

# --  matplotlib setting

# ---- font
font_path = "C:/Windows/Fonts/malgun.ttf"
font = font_manager.FontProperties(fname=font_path).get_name()
rc('font', family=font)

@errorLoggingDecorator
def scenarioVisualizingRoadTypeL(db):
    res,cols = db.executeQueryHasReturn({'q':"""select
       rtl.road_form_l as "도로형태",
       sum(c.death) as "총 사망자 수",
       sum(c.slight) as "총 중상자 수",
       sum(c.injured)+sum(c.slight)+sum(c.wound) as "총 부상자 수"
from accident a
         join casualty c on c.accident_id = a.accident_id
         join road_type_l rtl on a.road_type_l_id = rtl.road_type_l_id
        group by rtl.road_form_l;"""})
    
    df = pd.DataFrame(res,columns = cols)
    
    df.plot(kind='bar',x='도로형태',y='총 사망자 수')
    plt.show()
    
    df.plot(kind='bar',x='도로형태',y ='총 중상자 수')
    plt.show()
    
    df.plot(kind='bar',x='도로형태',y ='총 부상자 수')
    plt.show()

@errorLoggingDecorator
def scenarioVisualizingRoadType(db):
    res,cols = db.executeQueryHasReturn({'q':"""select
       rt.road_form as "도로형태",
       sum(c.death) as "총 사망자 수",
       sum(c.slight) as "총 중상자 수",
       sum(c.injured)+sum(c.slight)+sum(c.wound) as "총 부상자 수"
from accident a
         join casualty c on c.accident_id = a.accident_id
         join road_type rt on a.road_type_id = rt.road_type_id
        group by rt.road_form;"""})
    
    df = pd.DataFrame(res,columns = cols)
    
    df.plot(kind='bar',x='도로형태',y='총 사망자 수')
    plt.show()
    
    df.plot(kind='bar',x='도로형태',y ='총 중상자 수')
    plt.show()
    
    df.plot(kind='bar',x='도로형태',y ='총 부상자 수')
    plt.show()