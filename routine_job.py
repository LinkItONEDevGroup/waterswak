# -*- coding: UTF8 -*-
import getopt
import sys
import pandas as pd
from codes.db import *
from codes.lib import *

def job_daily_general(conn):
    filename="output/cwms_hsinchu.xml"
    if 1:
        url="HTTP://hsinchuauto.tk/cwmsopendata/cwms.xml"
        url_get(filename,url,reload=True)
        df=xml_to_df(filename)

        mdate=df['M_DATE'].unique()[0]
        sql="delete from e_cwms_hsinchu where \"M_DATE\"='%s'" %(mdate)
        sql_exec(conn,sql)
        df_to_db('e_cwms_hsinchu',df,'append') # first record need manual run as 'replace' or delete all records in table

try:
    opts, args = getopt.getopt(sys.argv[1:], "h", [])
    for opt, arg in opts:
        if opt == '-h':
            print ('period_job.py [ -h ] action_str [par1] [par2] ...')
            print("""ex:
    period_job.py daily-general""")

            sys.exit()
except getopt.GetoptError:
    print ('usage: period_job.py [ -h ]')
    sys.exit(2)



if len(args)==0:
    print ('usage: period_job.py [ -h ] action_str [par1] [par2] ...')
    sys.exit(2)
conn=connect_db()
act_str=args[0]
pars=[]

if act_str=="daily-general":
    job_daily_general(conn)