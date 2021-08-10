# @file riverlog_for_gis.py
# @brief riverlog related library, share with DevZone
# @author wuulong@gmail.com
import requests
import json
import os
import pandas as pd
from datetime import timedelta, datetime,date
import time
from pandas.api.types import is_numeric_dtype


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
        

      
def load_json_local(filename,file_id):
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
        df = load_json_local(filename,api_id)
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

def get_value_by_index(df,keyvalue, target_col):
    """
    find df's column(key) = value, return value of target_col
    keyvalue: col_name=value
    """
    cols = keyvalue.split("=")
    if len(cols)!=2:
        return ""
    keyvalue_key = cols[0]
    keyvalue_value = cols[1]
    if is_numeric_dtype(df[keyvalue_key]):
        keyvalue_value=float(cols[1])
    if not target_col in df.columns:
        return ""

    values = df[df[keyvalue_key]==keyvalue_value][target_col].values.tolist()
    if len(values)>0:
        value = values[0]
    else:
        value = ""
    return value

#----- 多水庫庫容百分比分析
def reservoir_load(bag,date_start, date_end, reservoir_list): #[10405,10201,10205]
    df_info = api_to_csv("reservoir-info",None)
    filename=api_to_csv_range(date_start,date_end,"reservoir-reservoirData",None,"ObservationTime")
    dest_name="%s_GMT8.csv" %(filename[:-4])
    df=csv_add_gmt8(filename,"ObservationTime", dest_name )

    #handle info
    df_info=df_info[df_info['Year']==105]
    df_info.drop_duplicates(subset="id")
    df_info["id"] = pd.to_numeric(df_info["id"])

    #merge/filter
    df2=df.merge(df_info, how='left', left_on='ReservoirIdentifier', right_on='id')
    df2=df2.drop_duplicates(subset=["ObservationTime","ReservoirIdentifier"],keep='last')
    df2=df2[df2['ReservoirIdentifier'].isin(reservoir_list)] #,20101,20201

    #Calculate, Pivot
    df2["ObservationTimeGMT8"] = pd.to_datetime(df2['ObservationTimeGMT8'])
    df2['percent']=df2['EffectiveWaterStorageCapacity']/df2['EffectiveCapacity']*100
    df2=df2[df2['percent']<=100]
    df3 = df2.pivot(index='ObservationTimeGMT8', columns='ReservoirName', values='percent')

    bag['reservoir-info']=df_info
    bag['reservoir-reservoirData']=df2
    bag['reservoir_pivot']=df3

def reservoir_plot(bag):
    #plot
    #%matplotlib notebook
    import matplotlib.pyplot as plt
    from matplotlib.font_manager import FontProperties
    myfont = FontProperties(fname=r'/Library/Fonts/Microsoft/SimSun.ttf')
    df = bag['reservoir_pivot']
    df.plot()
    plt.title("多水庫2021庫容比例",fontproperties=myfont)
    plt.legend(prop=myfont)
    plt.xticks(fontname = 'SimSun',size=8)
    plt.yticks(fontname = 'SimSun',size=8)
    plt.xlabel('時間',fontproperties=myfont)
    plt.ylabel('百分比',fontproperties=myfont)
    plt.show

#----- 今日淹水
def flood_load(bag,date_str,limit=0):

    #load 測站縣市補充資料
    df_info_縣市鄉鎮 = pd.read_csv("flood-station_縣市鄉鎮.csv")

    #get data, process
    df_info=api_to_csv("flood-station",None)
    df_info=df_info.merge(df_info_縣市鄉鎮, how='left', left_on='_id', right_on='_id')
    df_info

    #date_str = date.today() # 2021-06-07
    print("Today is %s" %(date_str))
    df = api_to_csv("flood-floodData",[date_str])

    df["timeGMT8"] = df['time'].apply(date_to_gmt8)
    df["timeGMT8"] = pd.to_datetime(df['timeGMT8'])

    df=df.merge(df_info_縣市鄉鎮, how='left', left_on='stationID', right_on='_id')
    df=df.drop_duplicates(subset=["time","stationName"],keep='last')
    df['stationName_city']=df['COUNTYNAME']  + '|' + df['TOWNNAME']  + '|' +  df['stationName']

    #filter, sort
    df=df[df['value']>=limit] #可改淹水高度, 有很多淹水資料時，改高一點比較不會太多
    df.sort_values(by=['timeGMT8'])

    bag['flood-station_縣市鄉鎮']=df_info_縣市鄉鎮
    bag['flood-station']=df_info
    bag['flood-floodData']=df

def flood_plot(bag,date_str):
    #%matplotlib notebook

    import matplotlib.pyplot as plt
    from matplotlib.font_manager import FontProperties

    df = bag['flood-floodData']

    myfont = FontProperties(fname=r'/Library/Fonts/Microsoft/SimSun.ttf')
    df2 = df.pivot(index='timeGMT8', columns='stationName_city', values='value')
    df2.plot(style='.-')
    title = "今日 %s 淹水感測器淹水值" %(date_str)
    plt.title(title,fontproperties=myfont)
    plt.legend(prop=myfont)
    plt.xticks(fontname = 'SimSun',size=8)
    plt.yticks(fontname = 'SimSun',size=8)
    plt.xlabel('時間',fontproperties=myfont)
    plt.ylabel('公分',fontproperties=myfont)
    fig = plt.gcf()
    fig.set_size_inches(8.5, 4.5)
    plt.show

    #淹水測站列表
def flood_list(bag):
    df = bag['flood-floodData']
    ary = df['stationName_city'].unique()
    for name in ary:
        print(name)

#----- 雨量站相關
#列出新竹市測站
def rain_station_view():
    df_info = api_to_csv("rain-station",None)
    filter_city = df_info['city']=='新竹縣'
    df_info = df_info[filter_city]

    return df_info


def rain_load(bag, date_str,limit=0,reload=False):
    df_info = api_to_csv("rain-station",None)
    #date_str = date.today() # 2021-06-07
    print("Today is %s" %(date_str))
    df=api_to_csv("rain-rainData",[date_str,"20","26","120","122"],reload)

    df["timeGMT8"] = df['time'].apply(date_to_gmt8)
    df["timeGMT8"] = pd.to_datetime(df['timeGMT8'])

    df=df.merge(df_info, how='left', left_on='stationID', right_on='stationID')
    df=df.drop_duplicates(subset=["timeGMT8","stationID"],keep='last')
    df['stationName']=df['city']  + '|' + df['town']  + '|' +  df['name'] + '|' + df['stationID']

    #filter, sort
    df=df[df['now']>=limit] #可改雨量值, 有很多淹水資料時，改高一點比較不會太多
    df=df.sort_values(by=['timeGMT8','stationID'])

    bag['rain-station']=df_info
    bag['rain-rainData']=df



#今日雨量 pivot
def rain_plot(bag,date_str, user_df=None):
    import matplotlib.pyplot as plt
    from matplotlib.font_manager import FontProperties

    if user_df is None:
        df = bag['rain-rainData']
    else:
        df = user_df

    myfont = FontProperties(fname=r'/Library/Fonts/Microsoft/SimSun.ttf')
    df2 = df.pivot(index='timeGMT8', columns='stationName', values='now')
    df2.plot(style='.-')
    title = "今日 %s 雨量站值" %(date_str)
    plt.title(title,fontproperties=myfont)
    plt.legend(prop=myfont)
    plt.xticks(fontname = 'SimSun',size=8)
    plt.yticks(fontname = 'SimSun',size=8)
    plt.xlabel('時間',fontproperties=myfont)
    plt.ylabel('mm',fontproperties=myfont)
    fig = plt.gcf()
    fig.set_size_inches(8.5, 4.5)
    plt.show

def rain_hourdiff(bag, time_set,station_city):
    #時雨量
    df_info = bag['rain-station']
    df = bag['rain-rainData']

    #df_info.head()
    if station_city is None:
        stations = None

    else:
        f1=df_info['city'].isin(station_city)
        #df_info[f1].values.tolist()
        #df_info[f1]['city'].unique()
        stations = df_info[f1]['stationID'].tolist()
    #print(stations)

    #df.head()
    #time_set=['2021-06-10 15:00:00','2021-06-10 16:00:00']
    f_time=df['timeGMT8'].isin(time_set)
    if stations is None:
        df_f = df[f_time]
    else:
        f_station=df['stationID'].isin(stations)
        df_f = df[f_station & f_time]
    #df[f_station]
    #df['city'].unique()
    if len(df_f.index)>0:
        #print(df_f)
        df_f = df_f.drop_duplicates(['stationName','timeGMT8'])
        df_pivot = df_f.pivot(index='stationName', columns='timeGMT8', values='now')
        print("time_set=%s" %(time_set))
        #print(df_pivot)
        df_pivot['rain_1hour']=df_pivot[time_set[1]]-df_pivot[time_set[0]]
        bag['rain-hourdiff']=df_pivot
        return True
    else:
        print("no data!")
        return False
def to_slot_10min(t_src):
    #t_now = datetime.now()
    #t_added = timedelta(minutes = 10)
    #t_slot= t_src - t_added
    slot_min=int(int(t_src.minute/10)*10)
    #date_str="%i-%i-%i %i:%02i:00" %(t_src.year,t_src.month,t_src.day,t_src.hour,slot_min)
    date_str="%i-%02i-%02i %02i:%02i:00" %(t_src.year,t_src.month,t_src.day,t_src.hour,slot_min)
    return date_str

def get_2slot(t_src,hour):
    #print("t_src=%s" %(t_src))
    date_str = to_slot_10min(t_src)

    date_obj = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    date_obj2 = date_obj + timedelta(hours = hour)
    date_str2 = date_obj2.strftime("%Y-%m-%d %H:%M:%S")

    return [date_str,date_str2]

def rain_alarm_hour(bag,station_city,limit):
    rain_load(gd, date.today(),True)

    #time_set=['2021-06-10 15:00:00','2021-06-10 16:00:00']
    time_now = datetime.now()
    time_set = get_2slot(time_now-timedelta(minutes = 90),1)
    #print(time_set)
    #station_city=['新竹縣','新竹市']
    rain_hourdiff(gd,time_set,station_city)
    df_pivot = gd['rain-hourdiff']
    if len(df_pivot.index)>0:
        df_pivot=df_pivot[df_pivot['rain_1hour']>limit]
        df_pivot=df_pivot.sort_values(by=['rain_1hour'],ascending=False)

        print("-----\nMonitor time: %s : %s 雨量站時雨量 > %i mm -----\n" %(time_now.strftime("%Y-%m-%d %H:%M:%S"), station_city,limit))
        print(df_pivot)
    else:
        print("no data!")
def rain_day_max(bag,date_str,station_city):
    rain_load(bag, date_str,True)
    #station_city=['新竹縣','新竹市']

    df_info = gd['rain-station']
    df = gd['rain-rainData']

    #df_info.head()
    f1=df_info['city'].isin(station_city)
    #df_info[f1].values.tolist()
    #df_info[f1]['city'].unique()
    stations = df_info[f1]['stationID'].tolist()

    #f_time=df['timeGMT8'].isin(time_set)
    f_station=df['stationID'].isin(stations)
    df_f = df[f_station]


    df_agg=df_f.groupby('stationName').agg({'now': ['max']})
    bag['rain_day_max']=df_agg

def rain_load_range(bag,date_start, date_end, limit=0,reload=False):
    df_info = api_to_csv("rain-station",None)
    #date_str = date.today() # 2021-06-07
    #print("Today is %s" %(date_str))
    filename=api_to_csv_range(date_start,date_end,"rain-rainData",["20","26","120","122"],"time")
    dest_name="%s_GMT8.csv" %(filename[:-4])
    df=csv_add_gmt8(filename,"time", dest_name )

    #df=api_to_csv_range("rain-rainData",[date_str,"20","26","120","122"],reload)

    if 1:
        #df["timeGMT8"] = df['time'].apply(date_to_gmt8)
        df["timeGMT8"] = pd.to_datetime(df['timeGMT8'])

        df=df.merge(df_info, how='left', left_on='stationID', right_on='stationID')
        df=df.drop_duplicates(subset=["timeGMT8","stationID"],keep='last')
        df['stationName']=df['city']  + '|' + df['town']  + '|' +  df['name'] + '|' + df['stationID']

        #filter, sort
        df=df[df['now']>=limit] #可改雨量值, 有很多淹水資料時，改高一點比較不會太多
        df=df.sort_values(by=['timeGMT8','stationID'])

    bag['rain-station']=df_info
    bag['rain-rainData']=df

#-----觀察寶山第二水庫與雨量站的變化
def rain_reservoir_multi(date_range,station_list,reservoir_list):
    #多雨量站資料
    #date_range=['2021-05-26','2021-06-12']
    if 1:
        #rain_load(gd, date_str,limit=0,reload=True)
        rain_load_range(gd,date_range[0],date_range[1],0,True)
        #gd['rain-rainData']
        df_info = gd['rain-station']
        df1= gd['rain-rainData']

        #station_list=['C0D550','C1D410','01D100','C0D580']

        f1=df1['stationID'].isin(station_list)
        df1 = df1[f1]
        #print(df1.columns)

        #rain_plot(gd,date_str, df[f1])

    #寶二水庫
    if 1:
        reservoir_load(gd,date_range[0],date_range[1],reservoir_list)
        #reservoir_plot(gd)
        df2=gd['reservoir-reservoirData']
        #reservoir_list = [10405]
        #print(df2.columns)

    if 1:
        #%matplotlib notebook
        import matplotlib.pyplot as plt
        from matplotlib.font_manager import FontProperties

        myfont = FontProperties(fname=r'/Library/Fonts/Microsoft/SimSun.ttf')

        count_all = len(station_list)+len(reservoir_list)
        fig, axs = plt.subplots(count_all)
        title = "%s-%s 雨量站與水庫庫容" %(date_range[0],date_range[1])
        fig.suptitle(title,fontproperties=myfont)
        ax_cur=0

        for i in range(len(station_list)):
            #label = station_list[i]
            label = get_value_by_index(df1,"stationID=%s" %(station_list[i]), 'stationName')
            axs[ax_cur].set_ylabel(label,fontproperties=myfont) #,loc="top"
            #axs[i].plot(x, y)
            f1=df1['stationID']==station_list[i]
            df1[f1].plot(x='timeGMT8',y='now',ax=axs[ax_cur],label=label,color='red',legend=None) #no need to specify for first axi
            ax_cur+=1
        for j in range(len(reservoir_list)):
            label = reservoir_list[j]
            #label = get_value_by_index(df2,"ReservoirIdentifier=%i" %(reservoir_list[j]), 'ReservoirName')
            label = get_value_by_index(df2,"id=%i" %(reservoir_list[j]), 'ReservoirName')
            axs[ax_cur].set_ylabel(label,fontproperties=myfont) #,loc="top"
            #axs[j].plot(x, y)
            f2=df2['ReservoirIdentifier']==reservoir_list[j]
            #names = df2[f2]['ReservoirName'].tolist()
            df2[f2].plot(x='ObservationTimeGMT8',y='percent',ax=axs[ax_cur],label=label,color='blue',legend=None) #no need to specify for first axi
            ax_cur+=1

        #plt.title(title,fontproperties=myfont)
        plt.xticks(fontname = 'SimSun',size=10)
        plt.yticks(fontname = 'SimSun',size=10)
        fig = plt.gcf()
        fig.set_size_inches(12.0, 4*count_all)
        plt.show
#----- 水位站
#查水位站
#常用站點 '河川水位測站-內灣-1300H013','河川水位測站-經國橋-1300H017' , '河川水位測站-上坪-1300H014'
def waterLevel_view():
    df_info = api_to_csv("waterLevel-station",None) #'BasinIdentifier',ObservatoryName
    filter_river = df_info['RiverName']=='上坪溪'
    #filter_name = df_info['BasinIdentifier']=='1300H017'

    # ID 查名稱
    #value=get_value_by_index(df_info,"BasinIdentifier=1140H037", 'ObservatoryName')
    value=get_value_by_index(df_info,"ObservatoryName=河川水位測站-內灣-1300H013", 'BasinIdentifier')
    print(value)

    return df_info[filter_river]

#準備今天的資料
def waterLevel_load(bag,date_str,reload=False):
    df_info = api_to_csv("waterLevel-station",None) #'BasinIdentifier',ObservatoryName

    #date_str=date.today() #2021-06-08
    #date_str='2021-06-08'

    df=api_to_csv("waterLevel-waterLevelData",[date_str,"23","25","120","123"],reload) #'RecordTime', 'StationIdentifier', 'WaterLevel'


    df["RecordTimeGMT8"] = df['RecordTime'].apply(date_to_gmt8)
    df["RecordTimeGMT8"] = pd.to_datetime(df['RecordTimeGMT8'])

    df=df.merge(df_info, how='left', left_on='StationIdentifier', right_on='BasinIdentifier')
    df=df.drop_duplicates(subset=["RecordTimeGMT8","StationIdentifier"],keep='last')
    df['stationName']=df['StationIdentifier']  + '|' + df['ObservatoryName']

    #filter, sort
    df=df[df['WaterLevel']>0] #可改水位值, 有很多淹水資料時，改高一點比較不會太多

    df=df.sort_values(by=['RecordTimeGMT8','StationIdentifier'])

    bag['waterLevel-station']=df_info
    bag['waterLevel-waterLevelData']=df

#畫單站圖表
def waterLevel_plotA(bag, StationId):
    #%matplotlib notebook
    import matplotlib.pyplot as plt
    from matplotlib.font_manager import FontProperties

    df = bag['waterLevel-waterLevelData']
    filter_river = df['StationIdentifier']==StationId
    #filter_river = df['RiverName'].str.contains('頭前溪', na=False)

    df = df[filter_river]

    myfont = FontProperties(fname=r'/Library/Fonts/Microsoft/SimSun.ttf')
    df2 = df.pivot(index='RecordTimeGMT8', columns='stationName', values='WaterLevel')
    df2.plot(style='.-')
    title = "今日 %s 水位值" %(date_str)
    plt.title(title,fontproperties=myfont)
    plt.legend(prop=myfont)
    plt.xticks(fontname = 'SimSun',size=8)
    plt.yticks(fontname = 'SimSun',size=8)
    plt.xlabel('時間',fontproperties=myfont)
    plt.ylabel('米',fontproperties=myfont)
    fig = plt.gcf()
    fig.set_size_inches(8.0, 4.5)
    plt.show

#兩個水位站畫在一張圖上
def waterLevel_plotB(bag, river_pair):
    #%matplotlib notebook
    import matplotlib.pyplot as plt
    from matplotlib.font_manager import FontProperties

    df_info = bag['waterLevel-station']
    df = bag['waterLevel-waterLevelData']

    #river_pair=['河川水位測站-內灣-1300H013','河川水位測站-經國橋-1300H017' ] #河川水位測站-上坪-1300H014

    river_pair.append(get_value_by_index(df_info,"ObservatoryName="+river_pair[0], 'BasinIdentifier'))
    river_pair.append(get_value_by_index(df_info,"ObservatoryName="+river_pair[1], 'BasinIdentifier'))

    river1 = df['BasinIdentifier']==river_pair[2+0]
    river2 = df['BasinIdentifier']==river_pair[2+1]
    river_both = river1 | river2

    df = df[river_both]

    myfont = FontProperties(fname=r'/Library/Fonts/Microsoft/SimSun.ttf')

    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()

    ax1.set_ylabel(river_pair[0],fontproperties=myfont) #,loc="top"

    df[river1].plot(x='RecordTimeGMT8',y='WaterLevel',ax=ax1,label=river_pair[0],color='red',legend=None) #no need to specify for first axis

    ax2.set_ylabel(river_pair[1],fontproperties=myfont)
    df[river2].plot(x='RecordTimeGMT8',y='WaterLevel',ax=ax2,label=river_pair[1],color='blue',legend=None)

    title = "今日 %s 水位值, 紅左藍右" %(date_str)
    plt.title(title,fontproperties=myfont)
    plt.xticks(fontname = 'SimSun',size=8)
    plt.yticks(fontname = 'SimSun',size=8)
    fig = plt.gcf()
    fig.set_size_inches(7.0, 5)
    plt.show

#----- 單雨量站+單水位站 混合圖
def rain_load1(bag, date_str,reload=False):
    df_info = api_to_csv("rain-station",None)
    #date_str = date.today() # 2021-06-07
    print("Today is %s" %(date_str))
    df=api_to_csv("rain-rainData",[date_str,"23","25","121","122"],reload)

    df["timeGMT8"] = df['time'].apply(date_to_gmt8)
    df["timeGMT8"] = pd.to_datetime(df['timeGMT8'])

    df=df.merge(df_info, how='left', left_on='stationID', right_on='stationID')
    df=df.drop_duplicates(subset=["timeGMT8","stationID"],keep='last')
    df['stationName']=df['city']  + '|' + df['town']  + '|' +  df['name'] + '|' + df['stationID']

    #filter, sort
    #df=df[df['now']>10] #可改雨量值, 有很多淹水資料時，改高一點比較不會太多
    df=df.sort_values(by=['timeGMT8','stationID'])

    bag['rain-station']=df_info
    bag['rain-rainData']=df

def rain_waterLevel_plot(bag,pair):

    #%matplotlib notebook
    import matplotlib.pyplot as plt
    from matplotlib.font_manager import FontProperties

    #pair=['內灣國小',河川水位測站-內灣-1300H013']

    myfont = FontProperties(fname=r'/Library/Fonts/Microsoft/SimSun.ttf')

    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()

    #ax2 先做就正常 [FIXME]
    ax2.set_ylabel(pair[1],fontproperties=myfont)
    bag['df2'].plot(x='RecordTimeGMT8',y='WaterLevel',ax=ax2,label=pair[1],color='blue',legend=None)

    ax1.set_ylabel(pair[2+0],fontproperties=myfont) #,loc="top"
    bag['df1'].plot(x='timeGMT8',y='now',ax=ax1,label=pair[2+0],color='red',legend=None) #no need to specify for first axis



    title = "今日 %s 雨量/水位值, 紅左藍右" %(date_str)
    plt.title(title,fontproperties=myfont)
    plt.xticks(fontname = 'SimSun',size=8)
    plt.yticks(fontname = 'SimSun',size=8)
    fig = plt.gcf()
    fig.set_size_inches(7.0, 5)
    plt.show

#----- 時間差變化
# 今日水位差

def mydiff(series):
    values = series.tolist()
    return max(values)-min(values)


def waterLevel_diff(bag,date_str,filter_def=None): #BasinIdentifier,RiverName,stationName
    waterLevel_load(gd,date_str,True)
    df_waterLevel_info=gd['waterLevel-station']
    df_waterLevel = gd['waterLevel-waterLevelData']
    #print(df_waterLevel.columns)

    if filter_def:
        cols=filter_def.split("=")
        #f1=df_waterLevel[cols[0]]==cols[1]
        f1=df_waterLevel[cols[0]].str.contains(cols[1], na=False) #RiverName 欄位值似乎有問題

        df=df_waterLevel[f1]
    else:
        df=df_waterLevel

    df_agg=df.groupby('stationName').agg({'WaterLevel': ['max',mydiff]})
    return df_agg

def riverlog_rain_hourdiff_mon(bag,limit):
    date_str= date.today()
    station_city=None
    time_now = datetime.now()
    time_set = get_2slot(time_now-timedelta(minutes = 80),1)
    print("time_set=%s" %(time_set))
    #time_set=['2021-06-11 08:00:00','2021-06-11 09:00:00']
    rain_load(bag, date_str,0,True)
    ret = rain_hourdiff(bag,time_set,station_city)
    if ret:
        df = bag['rain-hourdiff']
        df_cal = df[df['rain_1hour']>=limit]
        overlimit_cnt  = len(df_cal.index)
        print(df_cal)
    else:
        overlimit_cnt=0
    result = [time_set[1], overlimit_cnt]
    return result



