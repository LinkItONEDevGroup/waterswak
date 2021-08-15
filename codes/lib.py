# @file lib.py
# @brief misc library
# @author wuulong@gmail.com
import pandas as pd
from codes.db import *
from codes.riverlog import *
import pandasql as ps
from pandas.api.types import is_numeric_dtype

sql_keys={
    "rain_station":"select * from r_rain_station order by \"stationID\"",
    "info_tables":"select * from information_schema.tables where table_schema ='public' order by table_name",
    "info_columns":"select * from information_schema.columns",
    "table_desc":"select * from s_table_desc order by table_name"

}

sql_keypairs = {"basin_test1":"select * from public.basin where basin_name in (%s)",
         "basin_test2":"select * from public.basin where basin_name in ('%s','%s')",
         "basin_test3":"select * from public.basin where basin_name in ('%s','%s','%s')",
         "station_by_basinname":
             "select * from public.rivwlsta_e \
                where ST_Within(ST_SetSRID(geom,3826),(select ST_SetSRID(geom,3826) \
                 from public.basin where basin_name ='%s'))"}


def load_ods(bag,show_msg=1):
    ods_name="sys"
    sys_sts = pd.read_excel("include/%s.ods" %(ods_name), engine="odf",sheet_name=None)
    if show_msg:
        print("%s 的 sheets:\n %s " % (ods_name,sys_sts.keys()))
    bag[ods_name]=sys_sts

    ods_name="basic"
    basic_sts = pd.read_excel("include/%s.ods" %(ods_name), engine="odf",sheet_name=None)
    if show_msg:
        print("%s 的 sheets:\n %s " % (ods_name,basic_sts.keys()))
    bag[ods_name]=basic_sts

def api_to_csv_rename(name, new_name,par):
    df = api_to_csv(name,par)
    os.rename("output/%s.csv" %(name),"output/%s.csv" %(new_name))
    return df

# riverlog basic info setup
def riverlog_info_setup(bag):
    riverlog_sts={}
    riverlog_sts["r_rain_station"] = api_to_csv_rename("rain-station","r_rain_station",None)
    riverlog_sts["r_reservoir_info"] = api_to_csv_rename("reservoir-info","r_reservoir_info",None)
    riverlog_sts["r_waterlevel_station"] = api_to_csv_rename("waterLevel-station","r_waterlevel_station",None)
    riverlog_sts["r_waterleveldrain_station"] = api_to_csv_rename("waterLevelDrain-station","r_waterleveldrain_station",None)
    riverlog_sts["r_waterlevelagri_station"] = api_to_csv_rename("waterLevelAgri-station","r_waterlevelagri_station",None)
    riverlog_sts["r_sewer_station"] = api_to_csv_rename("sewer-station","r_sewer_station",None)
    riverlog_sts["r_tide_station"] = api_to_csv_rename("tide-station","r_tide_station",None)
    riverlog_sts["r_pump_station"] = api_to_csv_rename("pump-station","r_pump_station",None)
    riverlog_sts["r_reservoir_info"] = api_to_csv_rename("reservoir-info","r_reservoir_info",None)
    riverlog_sts["r_flood_station"] = api_to_csv_rename("flood-station","r_flood_station",None)
    print("riverlog 的 sheets:\n %s " % (riverlog_sts.keys()))


    bag['riverlog']=riverlog_sts

def view_by_key(conn,key):
    global sql_keys
    if key is None:
        return sql_keys
    if key in sql_keys.keys():
        sql = sql_keys[key]
    else:
        sql = "select * from %s" %(key)
    df = sql_to_df(conn,sql)
    return df

def get_table_list():
    df = view_by_key(conn,'info_tables')
    return df['table_name'].to_list()

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

def key_to_value(conn,tb_name,key_column,key_value,value_column):
    """
        想知道某個 table 中的某個欄位為某個值得另一個欄位的值
        如 流域 ID 為 130000 的名稱
        EX: key_to_value(conn,'b_流域','basin_id',130000,"basin_cname")
    """
    df = view_by_key(conn,tb_name)
    return get_value_by_index(df,"%s=%s" %(key_column,key_value),value_column)


def key_to_value_help(conn,tb_name,key_column):
    """
        使用 key_to_value 時，會需要知道 table name, column name 與裡面的值，這會提供需要的幫助
        EX:
        key_to_value_help(conn,None,None)
        key_to_value_help(conn,'b_流域',None)
        key_to_value_help(conn,'b_流域','basin_id')
    """
    df = view_by_key(conn,'info_tables')
    tb_list=df['table_name'].to_list()

    if tb_name is None or (not tb_name in tb_list):
        print("table list:\n%s" %( "\n".join(tb_list)))
        return

    df1 = view_by_key(conn,'info_columns')

    cols_list=df1[df1['table_name']==tb_name]['column_name'].to_list()

    if key_column is None or (not key_column in cols_list):
        print("table %s column list:\n%s" %(tb_name, "\n".join(cols_list)))
        return

    df2=view_by_key(conn,tb_name)
    keys_list = df2[key_column].unique().tolist()
    print("table %s column %s key list:\n%s" %(tb_name, key_column, "\n".join(map(str, keys_list))))

def keypar_to_view(conn,key,pars,tran_id):
    """
    用 sql key 跟參數，來看資料
    tran_id:
        'str': 會組成一個字串
        'pos': 用位置的方式，直接去使用，目前限定最多四個
    怎麼給，要看不同 sql key 的定義
    """
    global sql_keypairs
    if not key in sql_keypairs.keys():
        return None

    if not tran_id in ['str','pos']:
        return None

    sql_def = sql_keypairs[key]
    if tran_id=="str":
        par_str=""
        for par in pars:
            par_str += "'%s'," % (par)
        par_str = par_str[:-1]
        sql = sql_def % (par_str)
    if tran_id=="pos":
        if len(pars)>4:
            return None
        if len(pars)==1:
            sql = sql_def % (pars[0])
        if len(pars)==2:
            sql = sql_def % (pars[0],pars[1])
        if len(pars)==3:
            sql = sql_def % (pars[0],pars[1],pars[2])
        if len(pars)==4:
            sql = sql_def % (pars[0],pars[1],pars[2],pars[3])

    df = sql_to_df(conn,sql)
    return df

def walk_dir(search_dir,ext=".shp",white_list=None):
    """
    預設是找到目錄內有的 .shp 檔案的 pathname 回傳
    如果有提供 white_list, 則只會找到那些檔案。用在新增幾個檔案要匯入的情況
    """
    pathnames = []
    for dirpath, dirnames, filenames in os.walk(search_dir):
        #print("dirpath=%s\ndirnames=%s\nfilenames=%s" %(dirpath, dirnames, filenames))
        for filename in [f for f in filenames if f.endswith(ext)]:
            if not white_list is None:
                if filename in white_list:
                    pathnames.append(os.path.join(dirpath, filename))
                pass
            else:
                pathnames.append(os.path.join(dirpath, filename))
    return pathnames


def shp_tosql(lines,table_name, sql_path="data/shp.sql", srid=3826, encoding="UTF-8"):
    """將 shp 檔名s 產生出所需灌入資料庫的 script, 將輸出到命令列執行
    即可順利的將資料灌入資料庫
    """
    #lines=file_to_lines("shp.txt")
    if os.path.exists(sql_path):
        #os.remove(sql_path)
        print("rm %s" %(sql_path))

    for line in lines:
        #print(line,end = '')
        basename = os.path.basename(line)
        base = os. path. splitext(basename)[0]
        if table_name=="":
            out1 = "shp2pgsql -W %s -D -s %i -I \"%s\" \"%s\">> %s" %(encoding,srid,line,base,sql_path)
        else:
            out1 = "shp2pgsql -W %s -D -s %i -I \"%s\" \"%s\">> %s" %(encoding,srid,line,table_name,sql_path)

        print("%s" %(out1))
    out2 = "psql -p 5431 -U postgres -d postgis -f %s" %(sql_path)
    print("%s" %(out2))

def list_remove(lines,black_strs):
    """ list 中如果有 black_strs 的字串，就會被移除，例如用在找到的檔案中，某些不需要
    """
    ret = []
    for line in lines:
        for black_str in black_strs:
            if line.find(black_str)>=0:
                pass
            else:
                ret.append(line)
    return ret

def url_get(filename,url,reload=False):
    if not os.path.isfile(filename) or reload:
        print("Getting %s to %s" %(url,filename))
        t1 = datetime.now()
        r = requests.get(url, params = {})
        open(filename, 'wb').write(r.content)
        t2 = datetime.now()
        #print("[%s]-%s" %(t2-t1,filename))

def load_json(filename):
    data = None
    try:
        data_date = ""
        with open(filename , 'r', encoding='UTF-8') as json_file:
            data = json.load(json_file)
            return data
    except:
        print("%s:%s" %(filename,"EXCEPTION!"))
        return None
def xml_to_df(filename):
    import pandas as pd
    import xml.etree.ElementTree as ET

    tree = ET.parse(filename)
    root = tree.getroot()

    dfcols=[]
    sers = []
    for Data in root:
        rec_list=[]
        dfcols1=[]
        for record in Data:
            #print(record.tag,record.text)
            if not record.tag in dfcols:
                dfcols.append(record.tag)
            dfcols1.append(record.tag)
            rec_list.append(record.text)

        #print("rec_list=%s" %(rec_list))
        ser = pd.Series(rec_list, index=dfcols1)
        sers.append(ser)

    df = pd.DataFrame(columns=dfcols)
    for ser in sers:
        df = df.append(ser,ignore_index=True)
    return df

def sqldf(df,sql,filename=''):
    try:
        m1_df= ps.sqldf(sql, locals())
        print(m1_df)
        if not filename=='':
            m1_df.to_csv(filename)
            print("CSV file saved: %s" %(filename))
        return m1_df
    except:
        print("EXCEPTION: sql=%s" %(sql))
