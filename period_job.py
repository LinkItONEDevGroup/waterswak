# -*- coding: UTF8 -*-

import time
import random
import sys
import pandas as pd
from codes.db import *
from codes.lib import *
from codes.riverlog import *
import pandasql as ps

from datetime import datetime,date

gd={}
conn=connect_db()

case_id="riverlog_rain_hourdiff_station_cnt"
#case_id="pm25"
period=60 # second
while True:
    t_value = time.time()
    v=random.randint(0,99)
    sqls=[]
    if case_id=="test":
        sql="INSERT INTO t_time_series VALUES (%i,'test1', %i);" %(t_value,v)
        period=1
    if case_id=="pm25":
        url="https://pm25.lass-net.org/data/last.php?device_id=74DA38B053E4"
        filename="output/pm25_last.json"
        url_get(filename, url,True)
        data = load_json(filename)
        pm25=data['feeds'][0]['AirBox']['s_d0']
        sql="INSERT INTO t_time_series VALUES (%i,'pm25_74DA38B053E4', %.2f);" %(t_value,pm25)
        period=300
    if case_id=="riverlog_rain_hourdiff_station_cnt":
        try:
            resultA = riverlog_rain_hourdiff_mon(gd,10)
            t_id="riverlog_rain_hourdiff_station_10cnt"
            print("%s=%s" %(t_id,resultA))
            date_obj = datetime.strptime(resultA[0], "%Y-%m-%d %H:%M:%S")
            t_value = date_obj.timestamp()
            sqls.append("delete from t_time_series where dt=%i" %(t_value))
            sqls.append("INSERT INTO t_time_series VALUES (%i,'%s', %.2f);" %(t_value,t_id,resultA[1]))

            df = gd['rain-hourdiff']
            df_cal = df[df['rain_1hour']>=40]
            overlimit_cnt  = len(df_cal.index)

            t_id="riverlog_rain_hourdiff_station_40cnt"
            print("%s:" %(t_id))
            print(df_cal)

            #date_obj = datetime.strptime(resultB[0], "%Y-%m-%d %H:%M:%S")
            #t_value = date_obj.timestamp()
            #sqls.append("delete from t_time_series where dt=%i" %(t_value))
            sqls.append("INSERT INTO t_time_series VALUES (%i,'%s', %.2f);" %(t_value,t_id,overlimit_cnt))

        except ValueError:
            print(ValueError)
        except:
            print("Unexpected error:", sys.exc_info()[0])
        period=10*60

    if len(sqls)>0:
        for sql in sqls:
            print(sql)
            sql_exec(conn,sql)
    else:
        print(sql)
        sql_exec(conn,sql)

    time.sleep(period)
