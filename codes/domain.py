# @file domain.py
# @brief basic domain support
# @author wuulong@gmail.com

#standard
#extend
import pandas as pd
import pandasql as ps
#library
import codes.globalclasses as gc
from codes.const import *
from codes.db import *
from codes.lib import *

class MasterDetail():
    def __init__(self):
        self.m_df = None
    def init(self,conn,sql):
        self.m_df = sql_to_df(conn,sql)
    def mget(self,col_id):
        col_s = self.m_df[col_id]
        if len(col_s.index)>0:
            return col_s.tolist()[0]
        else:
            return None