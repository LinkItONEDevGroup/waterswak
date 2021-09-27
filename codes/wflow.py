# @file wflow.py
# @brief handle flow information
# @author wuulong@gmail.com

import netCDF4 as nc
import numpy as np
import datetime as dt
import cftime
from dateutil import tz

def nc_to_localtime(d):
    utc=dt.datetime(d.year, d.month, d.day, d.hour, d.minute, d.second)
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal()

    utc = utc.replace(tzinfo=from_zone)

    # Convert time zone
    local = utc.astimezone(to_zone)
    #print(dt1_local.strftime("%Y%m%dT%H%M%S+8"))
    return local

def walktree(top):
    yield top.groups.values()
    for value in top.groups.values():
        yield from walktree(value)

def to_datetime(d):

    if isinstance(d, dt.datetime):
        return d
    if isinstance(d, cftime.DatetimeNoLeap):
        return dt.datetime(d.year, d.month, d.day, d.hour, d.minute, d.second)
    elif isinstance(d, cftime.DatetimeGregorian):
        return dt.datetime(d.year, d.month, d.day, d.hour, d.minute, d.second)
    elif isinstance(d, str):
        errors = []
        for fmt in (
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%SZ"):
            try:
                return dt.datetime.strptime(d, fmt)
            except ValueError as e:
                errors.append(e)
                continue
        raise Exception(errors)
    elif isinstance(d, np.datetime64):
        return d.astype(dt.datetime)
    else:
        raise Exception("Unknown value: {} type: {}".format(d, type(d)))
def force_in(ary,idx):
    if idx<0:
        return 0
    if idx>= len(ary):
        return len(ary)-1
    return idx
class WFlow():
    def __init__(self,nc_file):
        #nc_file="/Users/wuulong/MakerBk2/QGIS/projects/Test_316/202107251400.nc"
        rootgrp = nc.Dataset(nc_file)
        self.flow_simulated = rootgrp.variables['flow_simulated']
        self.x = rootgrp.variables['x'][:]
        self.y = rootgrp.variables['y'][:]
        self.z = rootgrp.variables['z'][:]
        self.time = rootgrp.variables['time']
        self.time_d=nc.num2date(self.time,units = self.time.units,calendar='standard') # get date using NetCDF num2date function
        self.rootgrp=rootgrp
    def get_tyx(self,t,y,x): # 某個時間，經緯度的 flow
        #27114180, 24.4961,120.9339
        time_idx=np.searchsorted(self.time, t)
        y_idx=np.searchsorted(self.y, y)
        x_idx=np.searchsorted(self.x, x)
        return self.flow_simulated[time_idx][y_idx][x_idx]
    def get_his_tyx(self,y,x): # 某個經緯度的歷史流量
        #y=24.4961,x=120.9339
        y_idx=np.searchsorted(self.y,y )
        x_idx=np.searchsorted(self.x,x )

        print("time_str,y,x,flow")
        for i in range(len(self.time)):
            flow = self.flow_simulated[i][y_idx][x_idx]
            local = nc_to_localtime(self.time_d[i])
            time_str=local.strftime("%Y%m%dT%H%M%S+8")
            print("%s,%f,%f,%f" %(time_str,y,x,flow))
    def get_max_by_offset(self,y1,x1,o): # 某個點附近一定範圍內最高流量
        #y=24.4961,x=120.9339, o=5 #offset_idx
        y = self.y
        x= self.x
        y_idx=np.searchsorted(y,y1)
        x_idx=np.searchsorted(x,x1)
        print("y_idx=%i,x_idx=%i" %(y_idx,x_idx))
        area = self.flow_simulated[:,force_in(y,y_idx-o):force_in(y,y_idx+o),force_in(x,x_idx-o):force_in(x,x_idx+o)]
        area_max=np.max(area)
        print("x=%f,y=%f, offset=%i, max=%f" %(x1,y1,o,area_max))
