# @file riverlog_for_gis.py
# @brief riverlog related library, share with DevZone
# @author wuulong@gmail.com
import requests
import json
import os
import pandas as pd
from datetime import timedelta, datetime
import time


def url_get(filename,url,reload=False):
    """
        get url to file
    """
    #print("filename=%s,url=%s" %(filename,url))
    if os.path.isfile(filename) and reload==False:
        return
    else:
        r = requests.get(url, params = {})
        open(filename, 'wb').write(r.content)
        

      
def load_json(filename,file_id):
    """
        load json file and transfer to panda
        hardcode: handle json with data in 'data'  
    """
    with open(filename, 'r') as json_file:
        data_head = json.load(json_file)
    
    if file_id=="elev-gridData":
        data = data_head['data']['data']
    else:
        data = data_head['data']

    if len(data)>0:
        cols = data[0].keys()
    else:
        return None
        
    
    out = []
    for row in data:
        item = []
        for c in cols:
            item.append(row.get(c, {}))
        out.append(item)

    return pd.DataFrame(out, columns=cols)

def api_to_csv(api_id,pars,reload=False):
    """
        get api data and save to csv.
        api_id: api_path with '-' as delimiter
        pars = [], put parameters as string
    """
    api_map={
        "rain-dailySum":"https://riverlog.lass-net.org/rain/dailySum?year=%i",
        "rain-10minSum":"https://riverlog.lass-net.org/rain/10minSum?date=%s",
        "rain-station":"https://riverlog.lass-net.org/rain/station",
        "rain-rainData":"https://riverlog.lass-net.org/rain/rainData?date=%s&minLat=%s&maxLat=%s&minLng=%s&maxLng=%s",
        "waterLevel-station":"https://riverlog.lass-net.org/waterLevel/station",
        "waterLevel-waterLevelData":"https://riverlog.lass-net.org/waterLevel/waterLevelData?date=%s&minLat=%s&maxLat=%s&minLng=%s&maxLng=%s",
        "waterLevelDrain-station":"https://riverlog.lass-net.org/waterLevelDrain/station",
        "waterLevelDrain-waterLevelDrainData":"https://riverlog.lass-net.org/waterLevelDrain/waterLevelDrainData?date=%s&minLat=%s&maxLat=%s&minLng=%s&maxLng=%s",
        "waterLevelAgri-station":"https://riverlog.lass-net.org/waterLevelAgri/station",
        "waterLevelAgri-waterLevelAgriData":"https://riverlog.lass-net.org/waterLevelAgri/waterLevelAgriData?date=%s&minLat=%s&maxLat=%s&minLng=%s&maxLng=%s",
        "sewer-station":"https://riverlog.lass-net.org/sewer/station",
        "sewer-sewerData":"https://riverlog.lass-net.org/sewer/sewerData?date=%s&minLat=%s&maxLat=%s&minLng=%s&maxLng=%s",
        "tide-station":"https://riverlog.lass-net.org/tide/station",
        "tide-tideData":"https://riverlog.lass-net.org/tide/tideData?date=%s&minLat=%s&maxLat=%s&minLng=%s&maxLng=%s",
        "pump-station":"https://riverlog.lass-net.org/pump/station",
        "pump-pumpData":"https://riverlog.lass-net.org/pump/pumpData?date=%s&minLat=%s&maxLat=%s&minLng=%s&maxLng=%s",
        "reservoir-info":"https://riverlog.lass-net.org/reservoir/info",
        "reservoir-reservoirData":"https://riverlog.lass-net.org/reservoir/reservoirData?date=%s",
        "flood-station":"https://riverlog.lass-net.org/flood/station",
        "flood-floodData":"https://riverlog.lass-net.org/flood/floodData?date=%s",
        "alert-alertData":"https://riverlog.lass-net.org/alert/alertData?date=%s",
        "alert-alertStatistic":"https://riverlog.lass-net.org/alert/alertStatistic?year=%s",
        "alert-typhoonData":"https://riverlog.lass-net.org/alert/typhoonData?date=%s", # date can change to year
        "elev-gridData":"https://riverlog.lass-net.org/elev/gridData?level=%s&minLat=%s&maxLat=%s&minLng=%s&maxLng=%s",
        "statistic-waterUseAgriculture":"https://riverlog.lass-net.org/statistic/waterUseAgriculture",
        "statistic-waterUseCultivation":"https://riverlog.lass-net.org/statistic/waterUseCultivation",
        "statistic-waterUseLivestock":"https://riverlog.lass-net.org/statistic/waterUseLivestock",
        "statistic-waterUseLiving":"https://riverlog.lass-net.org/statistic/waterUseLiving",
        "statistic-waterUseIndustry":"https://riverlog.lass-net.org/statistic/waterUseIndustry",
        "statistic-waterUseOverview":"https://riverlog.lass-net.org/statistic/waterUseOverview",
        "statistic-monthWaterUse":"https://riverlog.lass-net.org/statistic/monthWaterUse",
        "statistic-reservoirUse":"https://riverlog.lass-net.org/statistic/reservoirUse",
        "statistic-reservoirSiltation":"https://riverlog.lass-net.org/statistic/reservoirSiltation"
        }
    url_fmt = api_map[api_id]
    
    if pars: 
        if len(pars)==1:
            url = url_fmt %(pars[0])  
            filename_prefix = "output/%s_%s" %(api_id,pars[0])
        elif api_id=="":
            pass
        else: #5 parameters
            url = url_fmt %(pars[0],pars[1],pars[2],pars[3],pars[4])  
            filename_prefix = "output/%s_%s" %(api_id,pars[0])            
    else: #None
        url = url_fmt  
        filename_prefix = "output/%s" %(api_id)
        
        
    cont = True
    while cont:
        filename = filename_prefix + ".json"
        url_get(filename,url,reload)
        df = load_json(filename,api_id)
        if df is None:
            print("%s don't have data" %(api_id))
            return None
        try:
            
            cont = False
        except:
            print("Exception when process %s, retrying after 60s" %(filename))
            if os.path.isfile(filename):
                os.remove(filename)
            time.sleep(60)
    filename = filename_prefix + ".csv"
    print("%s: %s saved, shape = %s" %(api_id,filename, str(df.shape)))
    df.to_csv(filename)
    return df

def proc_Sum(file_loc, file_src,file_row,file_geo):
    """
        process dailySum from columns to rows, by area , add location geo info   
            1: merge Sum/Num to Avg
            2. drop unused columns
            3. columns to rows
            4. merge geo info
        
    """
    df = pd.read_csv(file_src)
    df['central']=df['centralSum']/df['centralNum']
    df['north']=df['northSum']/df['northNum']
    df['south']=df['southSum']/df['southNum']
    
    df1=df.drop(['centralSum', 'centralNum','northSum','northNum','southSum','southNum','Unnamed: 0'], axis=1)
    
    df2 = (df1.set_index(["time"])
         .stack()
         .reset_index(name='Value')
         .rename(columns={'level_1':'location'}))
    
    df2.to_csv(file_row)
    df_geo = pd.read_csv(file_loc)
    
    
    df_merge = pd.merge(df2, df_geo, on='location')
    df_final = df_merge.sort_values(by='time')
    df_final.to_csv(file_geo)
    return df_final



def minSum_range(start_str,end_str):
    """
        get 10minSum by date range. merge to 1 CSV 
    """
    start_date = datetime.strptime(start_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_str, "%Y-%m-%d")

    first_day = True
    date_now = start_date
    df_all = None
    while True:
        if date_now > end_date:
            break
        month = date_now.month
        year = date_now.year
        file_datestr = date_now.strftime("%Y-%m-%d")
        df = api_to_csv("rain-10minSum",[file_datestr])
        #df['datetime']="%s" %(date_now.strftime("%Y/%m/%d"))
        if first_day:
            df_all = df
            first_day = False
        else:
            df_all = pd.concat([df_all,df])

        date_now += timedelta(days=1)
    
    filename = "output/%s_%s_%s.csv" %("rain-10minSum",start_str,end_str)
    print("rain-10minSum saved %s, shape = %s" %(filename, str(df_all.shape)))
    df_save = df_all.sort_values(by='time')
    df_save.to_csv(filename,header=True,float_format="%.2f")

def api_to_csv_range(start_str,end_str,api_id,pars,sort_col_name):
    """
        get api by date range. merge to 1 CSV
    """
    start_date = datetime.strptime(start_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_str, "%Y-%m-%d")

    first_day = True
    date_now = start_date
    df_all = None
    while True:
        if date_now > end_date:
            break
        month = date_now.month
        year = date_now.year
        file_datestr = date_now.strftime("%Y-%m-%d")
        if pars is None:
            df = api_to_csv(api_id,[file_datestr])
        else:
            real_pars = [file_datestr]
            real_pars.extend(pars)
            df = api_to_csv(api_id,real_pars)
        #df['datetime']="%s" %(date_now.strftime("%Y/%m/%d"))
        if first_day:
            df_all = df
            first_day = False
        else:
            df_all = pd.concat([df_all,df])

        date_now += timedelta(days=1)

    filename = "output/%s_%s_%s.csv" %(api_id,start_str,end_str)
    print("%s saved %s, shape = %s" %(api_id,filename, str(df_all.shape)))
    df_save = df_all.sort_values(by=sort_col_name)


    df_save.to_csv(filename,header=True,float_format="%.2f")
    return filename


def date_to_gmt8(date_str):
    """
        transfer date string to GMT+8
        ex: 2021-05-31T16:00:00.000Z
    """
    #date object
    date_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.000Z")

    #+8 hour
    hours_added = timedelta(hours = 8)
    date_gmt8 = date_obj + hours_added
    #output format
    date_ret = date_gmt8.strftime("%Y-%m-%d %H:%M:%S")
    return date_ret

def csv_add_gmt8(file_src,col_name,file_dest):
    """
        add GMT8 time to CSV by re-format one column

    """
    df = pd.read_csv(file_src)
    df[col_name + "GMT8"] = df[col_name].apply(date_to_gmt8)
    df_save=df.drop(['Unnamed: 0'], axis=1)
    df_save.to_csv(file_dest)
    return df_save





case_id=2 # 0: first version, 1: reservoir data by date, 2: for notebook debug

if case_id==0:
    if 1: #get each api to CSV
        api_to_csv("rain-dailySum",[2020])
        api_to_csv("rain-10minSum",["2020-09-01"])
        api_to_csv("rain-station",None)
        api_to_csv("rain-rainData",["2020-09-01","23","24","121","122"])
        api_to_csv("waterLevel-station",None)
        api_to_csv("waterLevel-waterLevelData",["2020-09-01","23","24","121","122"])
        api_to_csv("waterLevelDrain-station",None)
        api_to_csv("waterLevelDrain-waterLevelDrainData",["2019-12-03","23","24","120","122"])
        api_to_csv("waterLevelAgri-station",None)
        api_to_csv("waterLevelAgri-waterLevelAgriData",["2019-12-03","23","24","120","122"])
        api_to_csv("sewer-station",None)
        api_to_csv("sewer-sewerData",["2019-12-02","24","25","121","122"])
        api_to_csv("tide-station",None)
        api_to_csv("tide-tideData",["2020-09-01","23","24","121","122"])
        api_to_csv("pump-station",None)
        api_to_csv("pump-pumpData",["2019-12-03","25","26","121","122"])
        api_to_csv("reservoir-info",None)
        api_to_csv("reservoir-reservoirData",["2020-09-01"])
        api_to_csv("flood-station",None)
        api_to_csv("flood-floodData",["2020-09-01"])
        api_to_csv("alert-alertData",["2020-09-01"])
        api_to_csv("alert-alertStatistic",[2020])
        api_to_csv("alert-typhoonData",["2020-09-01"])
        api_to_csv("elev-gridData",["7","23","24","120","121"])
        api_to_csv("statistic-waterUseAgriculture",None)
        api_to_csv("statistic-waterUseCultivation",None)
        api_to_csv("statistic-waterUseLivestock",None)
        api_to_csv("statistic-waterUseLiving",None)
        api_to_csv("statistic-waterUseIndustry",None)
        api_to_csv("statistic-waterUseOverview",None)
        api_to_csv("statistic-monthWaterUse",None)
        api_to_csv("statistic-reservoirUse",None)
        api_to_csv("statistic-reservoirSiltation",None)

    if 1: #process rain-dailySum,10minSum , predefined 3 area geo definition: areaGeo.csv
        api_to_csv("rain-dailySum",[2020])
        proc_Sum("areaGeo.csv","output/rain-dailySum_2020.csv","output/rain-dailySum_2020_row.csv","output/rain-dailySum_2020_geo.csv")

        minSum_range("2020-10-01","2020-10-05")
        proc_Sum("areaGeo.csv","output/rain-10minSum_2020-10-01_2020-10-05.csv","output/rain-10minSum_2020-10-01_2020-10-05_row.csv","output/rain-10minSum_2020-10-01_2020-10-05_geo.csv")

if case_id==1:
    api_to_csv("reservoir-info",None)
    filename=api_to_csv_range("2021-01-01","2021-06-05","reservoir-reservoirData",None,"ObservationTime")
    csv_add_gmt8(filename,"ObservationTime","%s_GMT8.csv" %(filename[:-4]) )