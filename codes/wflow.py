# @file wflow.py
# @brief handle flow information
# @author wuulong@gmail.com

import netCDF4 as nc
import numpy as np
import datetime as dt
import cftime
from dateutil import tz

def nc_to_localtime(d): #time_d
    utc=dt.datetime(d.year, d.month, d.day, d.hour, d.minute, d.second)
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal()

    utc = utc.replace(tzinfo=from_zone)

    # Convert time zone
    local = utc.astimezone(to_zone)
    #print(dt1_local.strftime("%Y%m%dT%H%M%S+8"))
    return local
def localtime_to_nc(d): #datetime
    to_zone = tz.tzutc()
    from_zone = tz.tzlocal()

    local = d.replace(tzinfo=from_zone)
    utc = local.astimezone(to_zone)

    timestamp = dt.datetime.timestamp(utc)
    return int(timestamp/60)
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
    def get_flow(self,tid, par, b_print=True): # get flow by type
        """get flow by type
        tid : string, par=[] , b_print: need print
        time_x_y : [t,x,y ]  ex: [27114180, 120.9339,24.4961] , return float
        his_x_y : [x,y] ex: [120.9339,24.4961] , return [[]]  # 某個經緯度的歷史流量
        max_offset ; [x,y,o] ex: [120.9339,24.4961, 10] #offset_idx 實際範圍為 *2*20m, return float
        return value: different by tid
        """
        if tid=="time_x_y":
            t = par[0]
            x = par[1]
            y = par[2]
            time_idx=np.searchsorted(self.time, t)
            y_idx=np.searchsorted(self.y, y)
            x_idx=np.searchsorted(self.x, x)
            ret = self.flow_simulated[time_idx][y_idx][x_idx]
            if b_print:
                print(ret)
            return ret
        if tid=="his_x_y":
            x = par[0]
            y = par[1]
            y_idx=np.searchsorted(self.y,y )
            x_idx=np.searchsorted(self.x,x )
            lines = []
            if b_print:
                print("time_str,y,x,flow")
            for i in range(len(self.time)):
                flow = self.flow_simulated[i][y_idx][x_idx]
                local = nc_to_localtime(self.time_d[i])
                time_str=local.strftime("%Y%m%dT%H%M%S")
                line = "%s,%f,%f,%f" %(time_str,y,x,flow)
                if b_print:
                    print(line)
                lines.append([time_str,y,x,flow])
            return lines
        if tid=="max_offset":
            x1 = par[0]
            y1 = par[1]
            o = par[2]
            y = self.y
            x= self.x
            y_idx=np.searchsorted(y,y1)
            x_idx=np.searchsorted(x,x1)
            #if b_print:
            #    print("y_idx=%i,x_idx=%i" %(y_idx,x_idx))
            area = self.flow_simulated[:,force_in(y,y_idx-o):force_in(y,y_idx+o),force_in(x,x_idx-o):force_in(x,x_idx+o)]
            area_max=np.max(area)
            if b_print:
                print("x=%f,y=%f, offset=%i, max=%f" %(x1,y1,o,area_max))
            return area_max

    def get_flow_bylist(self,tid, pars, b_print=True): # call get_flow with list
        rets = []
        for par in pars:
            ret = self.get_flow(tid,par,b_print)
            rets.append(ret)
        return rets
    def desc(self):
        print("----- rootgrp -----\n%s" %(self.rootgrp))
        print("----- flow_simulated -----\n%s" %(self.flow_simulated))
        #print("----- time -----\n%s" %(self.time))
        print("----- x -----\n%s" %(self.x))
        print("----- y -----\n%s" %(self.y))
        print("----- z -----\n%s" %(self.z))
        #print("----- time_d -----\n%s" %(self.time_d))
        print("----- time info -----")
        print("index,time,local_time")
        for i in range(len(self.time)):
            local = nc_to_localtime(self.time_d[i])
            time_str=self.time[i]
            local_str=local.strftime("%Y%m%dT%H%M%S")
            line = "%i,%s,%s" %(i,time_str,local_str)
            print(line)