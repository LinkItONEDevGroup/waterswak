# @file cli.py
# @brief CLI of whole tool
# @author wuulong@gmail.com
#standard
import cmd
import logging

#extend
#import pandas as pd
#import pandasql as ps
import geopandas
from shapely.geometry import *

#library
import codes.globalclasses as gc
from codes.const import *
#from codes.db import *
from codes.lib import *
from codes.riverlog import *
from codes.tools import *
from codes.flwdir import *
import codes.wflow as wflow



DOMAIN_SET_LOAD_SKIP=0
DOMAIN_SET_LOAD_CSV=1
DOMAIN_SET_LOAD_GEO=2

CX_DICT_LOAD_SKIP=0

def path_sethome(pathname): # home: ~/ -> DATA2_DIR
    #pathname="水保局/水保局_110年度全臺839子集水區範圍圖/VA18049.kmz"
    #homepath="/Users/wuulong/MakerBk2/QGIS/projects/hackathon"
    homepath = gc.SETTING["DATA2_DIR"]
    if pathname[0:2]=="~/":
        pathname = "%s/%s" %(homepath, pathname[2:])
    return pathname

##### Code section #####
#Spec: about, user commands, test commands
#How/NeedToKnow:
class Cli(cmd.Cmd):
    def __init__(self,stdout=None):
        cmd.Cmd.__init__(self)
        self.cli_eng = CliEng(stdout)
        self.cli_tool = CliTool(stdout)
        self.cli_sys = CliSys(stdout)
        self.cli_basic = CliBasic(stdout)
        self.cli_basin = CliBasin(stdout)
        self.cli_river = CliRiver(stdout)
        self.cli_cx = CliCx(stdout)
        self.cli_sw = CliSourcingWater(stdout)

        self.prompt = 'WsCLI>'

############ cli maintain ####################        
    def do_about(self, line):
        """About this software"""
        print("%s version: v%s" %(TITLE,VERSION))
    def do_quit(self, line):
        """quit"""
        return True
############ top command ####################                      
    def do_reset(self,line):
        """ reset for next run """  
        gc.GAP.reset()
        print("reseted")
    def do_reload_setting(self,line):
        """reload setting from INI"""
        gc.SETTING.reload()
        pd.set_option('display.max_rows', int(gc.SETTING["MAX_ROWS"]))
        pd.set_option('display.max_columns', int(gc.SETTING["MAX_COLUMNS"]))
        pd.set_option('display.max_colwidth', int(gc.SETTING["MAX_COLWIDTH"]))

    def do_displayall(self,line):
        """set record display all or brief
        0: brief, 1: all, row: all rows, col: all columns
displayall [0/1/row/column]
ex: display 1
        """
        if line=="0":
            gc.SETTING.reload()
            pd.set_option('display.max_rows', int(gc.SETTING["MAX_ROWS"]))
            pd.set_option('display.max_columns', int(gc.SETTING["MAX_COLUMNS"]))
            pd.set_option('display.max_colwidth', int(gc.SETTING["MAX_COLWIDTH"]))
        elif line=="row":
            pd.set_option('display.max_rows', None)
        elif line=="col":
            pd.set_option('display.max_columns', None)
        else:
            pd.set_option('display.max_rows', None)
            pd.set_option('display.max_columns', None)
            #pd.set_option('display.max_colwidth', 0)

    def do_eng(self,line):
        """eng sub command directory"""
        gc.GAP.conn=connect_db()
        self.cli_eng.prompt = self.prompt[:-1]+':eng>'
        self.cli_eng.cmdloop()

    def do_tool(self,line):
        """tool sub command directory"""
        self.cli_tool.prompt = self.prompt[:-1]+':tool>'
        self.cli_tool.cmdloop()

    def do_sys(self,line):
        """sys sub command directory"""
        self.cli_sys.prompt = self.prompt[:-1]+':sys>'
        self.cli_sys.cmdloop()
    def do_basic(self,line):
        """basic sub command directory"""
        self.cli_basic.prompt = self.prompt[:-1]+':basic>'
        self.cli_basic.cmdloop()
    def do_basin(self,line):
        """basin sub command directory"""
        self.cli_basin.prompt = self.prompt[:-1]+':basin>'
        self.cli_basin.cmdloop()

    def do_river(self,line):
        """river sub command directory"""
        self.cli_river.prompt = self.prompt[:-1]+':river>'
        self.cli_river.cmdloop()

    def do_catchment(self,line):
        """catchment sub command directory"""
        self.cli_cx.prompt = self.prompt[:-1]+':catchment>'
        self.cli_cx.cmdloop()

    def do_sourcingwater(self,line):
        """sourcingwater sub command directory"""
        self.cli_sw.prompt = self.prompt[:-1]+':sourcingwater>'
        self.cli_sw.cmdloop()
############ eng sub cmd ####################
class CliEng(cmd.Cmd):
    def __init__(self,stdout):
        if stdout:
            cmd.Cmd.__init__(self,stdout=stdout)
        else:
            cmd.Cmd.__init__(self)

    def do_view_by_key(self, line):
        """ 看所有的 table 跟預設好的 sql
        沒給值會列出預設的 keys
        ex: view_by_key [rivercode]"""
        pars=line.split()
        if len(pars)==1:
            key = pars[0]
        else:
            dict = view_by_key(gc.GAP.conn,None)
            print("pre-defined keys=\n%s" %( dict.keys()))
            return
        df=view_by_key(gc.GAP.conn,key)
        print(df)

    def do_key_to_value(self, line):
        """
ex: key_to_value b_河川 river_id 130000.0 river_cname
        """
        tb_name=key_column=key_value=value_column=""
        pars=line.split()
        if len(pars)==4:
            tb_name = pars[0]
            key_column = pars[1]
            key_value = pars[2]
            value_column = pars[3]
        else:
            return
        print(key_to_value(gc.GAP.conn,tb_name,key_column,key_value,value_column))

    def do_key_to_value_help(self, line):
        """
ex: key_to_value_help [b_河川] [river_id]
        """
        tb_name=key_column=None
        pars=line.split()
        if len(pars)==2:
            tb_name = pars[0]
            key_column = pars[1]
        if len(pars)==1:
            tb_name = pars[0]
        key_to_value_help(gc.GAP.conn,tb_name,key_column)


    def do_quit(self, line):
        """quit this sub command"""
        if gc.GAP.conn:
            close_db(gc.GAP.conn)
        """quit"""
        return True

############ tool sub cmd ####################
class CliTool(cmd.Cmd):
    def __init__(self,stdout):
        if stdout:
            cmd.Cmd.__init__(self,stdout=stdout)
        else:
            cmd.Cmd.__init__(self)
        self.tdf = None # layer global dataframe
        self.get_flow_ret = None
    def do_read_file(self,line):
        """using geopandas read_file to load geo data file -> tool layer global dataframe(tdf).
(1)shp, gpkg, zip tested
(2)input should follow geopandas rule
(3)if path start with ~/ , will locate in DATA2_DIR(defined in waterswak.ini)
(4)geopandas read_file parameters needed can be pass by multiple keyword=value
(5)pathname=help provide keyword help message

read_file pathname [keyword=value] [...]
ex: read_file data/7441-鄉鎮市區界線(TWD97經緯度)/TOWN_MOI.shp  encoding='utf-8'
    read_file ~/國發會/TOWN_MOI/TOWN_MOI_1080617.shp

"""

        data_dir=''
        pars=line.split()
        dict_par={}
        if len(pars)>=2:
            for i in range(1,len(pars)):
                cols= pars[i].split("=")
                if len(cols)==2:
                    dict_par[cols[0]]=cols[1]
        if len(pars)>=1:
            if pars[0]=='help':
                import fiona;
                help(fiona.open)
                return
            elif pars[0][0:2]=="~/":
                data_dir=gc.SETTING['DATA2_DIR']
                filename = "%s/%s" %(data_dir,pars[0][2:])
            else:
                filename = pars[0]
        try:
            self.tdf = None
            if len(dict_par)>0:
                self.tdf = geopandas.read_file(filename,**dict_par)
            else:
                self.tdf = geopandas.read_file(filename,encoding='utf-8')
            gc.GAP.pd['cli_tool']=self.tdf

        except:
            print("Can't read_file: %s" %(filename)  )
        if not self.tdf is None:
            print(self.tdf)
    def do_tdf_act(self,line):
        """ apply action to tool layer dataframe (tdf)
tdf_act command
    print-print current dataframe, to_csv-save to CSV
ex:
tdf_act print
tdf_act to_csv TOWN_MOI.csv  #default in output directory
        """
        pars=line.split()
        if len(pars)>=2:
            act_value = pars[1]
        if len(pars)>=1:
            act_id = pars[0]

        if not act_id in ['print','to_csv']:
            print("not valid action!")
            return
        if not self.tdf is None:
            if act_id=="print":
                gc.GAP.desc_pd('cli_tool')
                #print(self.tdf)
            if act_id=="to_csv":
                filename="output/%s" %(act_value)
                self.tdf.to_csv(filename)
                print("CSV file saved: %s" %(filename))
    def do_rivercode_search(self,line):
        """search rivercode by position WGS84:
document: https://hackmd.io/@d9OQzm9mRx2Q-m9bpq7QDQ/SkPm0qQyY
rivercode_search x y
ex: rivercode_search 120.6666785 24.1755573
        """
        pars=line.split()
        if len(pars)==2:
            x = float(pars[0])
            y = float(pars[1])
        else:
            print("need 2 parameters")
            return
        data = rivercode_search([x,y])
        if data is None:
            print("search %f %f: can't get result, maybe too far from water " %(x,y))
        else:
            print("search %f %f: %s " %(x,y,data))


    def do_to_crs(self,line):
        """crs transfer
to_crs x y src_crs dst_crs
ex: to_crs 121.1359083 24.74512778 4326 3826
        """
        pars=line.split()
        if len(pars)==4:
            x = float(pars[0])
            y = float(pars[1])
            src_crs = int(pars[2])
            dst_crs = int(pars[3])
        else:
            return
        xy=to_crs([x, y], src_crs, dst_crs)
        print("(%f %f)@%i -> (%f %f)@%i" %(x,y,src_crs,xy[0],xy[1],dst_crs))
    def do_quit(self, line):
        """quit this sub command"""
        """quit"""
        return True
    def do_get_flow(self,line):
        """get flow data
get_flow flow_id desc
get_flow flow_id time_xy t x y o
get_flow flow_id his_xy x y o
get_flow flow_id max_offset x y o

ex: get_flow C1300QE-202107251400 desc
    get_flow C1300QE-202107251400 time_xy 27114180 120.9339 24.4961 10
    get_flow C1300QE-202107251400 his_xy 120.9339 24.4961 10
    get_flow C1300QE-202107251400 max_offset 120.9339 24.4961 10
        """
        self.get_flow_ret = ""
        pars=line.split()
        if len(pars)>=2:
            flow_id = pars[0]
            tid = pars[1]
        else:
            print("parameters count should >=2")
            return
        filename="include/flow_def.json"
        data = load_json(filename)
        flow_ids={}
        for i in range(len(data)):
            flow_ids[data[i]['flow_id']]=data[i]['nc_file']

        if flow_id in flow_ids.keys():
            nc_file=flow_ids[flow_id]
        else:
            print("flow_id list: %s" %(flow_ids.keys()))
            return
        wf=wflow.WFlow(nc_file)
        if tid=="desc":
            ret=wf.desc()
            self.get_flow_ret = "\n".join(ret)
        if tid=="time_xy":
            if len(pars)==6:
                t = float(pars[2])
                x = float(pars[3])
                y = float(pars[4])
                o = int(pars[5])
                ret=wf.get_flow(tid,[t,x,y,o],True)
                self.get_flow_ret = ret
            else:
                print("parameters count should = 6")
        if tid=="his_xy":
            if len(pars)==5:
                x = float(pars[2])
                y = float(pars[3])
                o = int(pars[4])
                ret=wf.get_flow(tid,[x,y,o],True)
                self.get_flow_ret = ret
            else:
                print("parameters count should = 5")
        if tid=="max_offset":
            if len(pars)==5:
                x = float(pars[2])
                y = float(pars[3])
                o = int(pars[4])
                ret=wf.get_flow(tid,[x,y,o],True)
                self.get_flow_ret = ret
            else:
                print("parameters count should = 5")
    def do_csv_get_flow(self,line):
        """get flow data from CSV data
csv_get_flow csv_path => result saved output/flow_query_result.csv
ex: csv_get_flow data/flow_query.csv
        """
        pars=line.split()
        if len(pars)>=1:
            csv_path = pars[0]
        else:
            print("parameters count should >=1")
            return
        if os.path.isfile(csv_path):
            pass
        else:
            print("%s not exist!" %(csv_path))
        df= pd.read_csv(csv_path)
        for index,row in df.iterrows():
            tid=row['tid']
            line=""
            if tid=='time_xy':
                line = "%s %s %s %s %s %i" %(row['flow_id'],row['tid'],row['t'],row['x'],row['y'],row['o'])
                self.do_get_flow(line)

                #print("get ret=%s" %(ret))
                df.loc[index, 'flow']=self.get_flow_ret
            if tid=='his_xy':
                line = "%s %s %s %s %i" %(row['flow_id'],row['tid'],row['x'],row['y'],row['o'])
                ret = self.do_get_flow(line)
                #print("get ret=%s" %(ret))
                df.loc[index, 'result']=str(self.get_flow_ret)
            if tid=='max_offset':
                line = "%s %s %s %s %i" %(row['flow_id'],row['tid'],row['x'],row['y'],row['o'])
                ret = self.do_get_flow(line)
                #print("get ret=%s" %(ret))
                df.loc[index, 'flow']=ret
            if tid=='desc':
                line = "%s %s" %(row['flow_id'],row['tid'])
                ret = self.do_get_flow(line)
                #print("get ret=%s" %(ret))
                df.loc[index, 'desc']=str(self.get_flow_ret)
        csv_result="output/flow_query_result.csv"
        df.to_csv(csv_result)
        print("%s output!" %(csv_result))

############ base class ####################
class CliTblBase(cmd.Cmd):
    #domain_set=['cli_basin','data/basin-河川流域範圍圖/basin-河川流域範圍圖.shp','NOGEOM',1]
    def __init__(self,stdout,domain_set):
        if stdout:
            cmd.Cmd.__init__(self,stdout=stdout)
        else:
            cmd.Cmd.__init__(self)
        self.df=None
        self.domain_set = domain_set
        self.load(domain_set[3])
    def load(self,load_type):
        """load default dataframe
        :param load_type: 0-not load, 1-pd.read_csv, 2-geopandas.read_file, ...
        :return: None
        """
        if load_type==1:
            self.df= pd.read_csv(self.domain_set[1])
            gc.GAP.pd[self.domain_set[0]]=self.df
        if load_type==2:
            self.df = geopandas.read_file(self.domain_set[1],encoding='utf-8')
            gc.GAP.pd['cli_basin']=self.df
    def do_sql(self,line):
        """apply sql to select
sql sql_cmd
ex: sql "select * from df where river_link like '0@130000%'"
        """
        sql = line.replace("\"","")
        df = self.df
        sqldf(df,sql)
    def do_desc(self,line):
        """desc layer"""
        if line=="1":
            gc.GAP.desc_pd(self.domain_set[0],'')
        else:
            gc.GAP.desc_pd(self.domain_set[0],self.domain_set[2])
    def do_quit(self, line):
        """quit this sub command"""
        """quit"""
        return True
############ sys sub cmd ####################
class CliSys(CliTblBase):
    def __init__(self,stdout):
        domain_set = ['sys','','',DOMAIN_SET_LOAD_SKIP]
        CliTblBase.__init__(self,stdout,domain_set)

    def do_sql(self,line):
        """apply sql to select
sql table_name sql_cmd
ex: sql table_def "select * from df where type='M'"
        """
        pars=line.split("\"")
        if len(pars)!=3:
            print("should follow the format")
            return
        table_name = pars[0].strip()
        if table_name in gc.GAP.pd[self.domain_set[0]].keys():
            sql = pars[1]
            df = gc.GAP.pd[self.domain_set[0]][table_name]
            filename=''
            if gc.SETTING["SQLDF_AUTOSAVE"]!="0":
                filename="output/sqldf_autosave_sys.csv"
            df_out = sqldf(df,sql,filename)

        else:
            print("table %s not exist!" %(table_name))
    def do_desc(self,line):
        """desc tables
desc [table_name]
    if table_name not exist, show table list
ex: desc table_def
        """
        pars=line.split()
        if len(pars)==0:
            print("table list:%s" %(gc.GAP.pd[self.domain_set[0]].keys()))
            return
        else:
            table_name = line
            if table_name in gc.GAP.pd[self.domain_set[0]].keys():
                df = gc.GAP.pd[self.domain_set[0]][table_name]
                print(df)
            else:
                print("table %s not exist!" %(table_name))

############ basic sub cmd ####################
class CliBasic(CliTblBase):
    def __init__(self,stdout):
        domain_set = ['basic','','',DOMAIN_SET_LOAD_SKIP]
        CliTblBase.__init__(self,stdout,domain_set)

    def do_sql(self,line):
        """apply sql to select
sql table_name sql_cmd
ex: sql table_def "select * from df where type='M'"
        """
        pars=line.split("\"")
        if len(pars)!=3:
            print("should follow the format")
            return
        table_name = pars[0].strip()
        if table_name in gc.GAP.pd[self.domain_set[0]].keys():
            sql = pars[1]
            df = gc.GAP.pd[self.domain_set[0]][table_name]
            filename=''
            if gc.SETTING["SQLDF_AUTOSAVE"]!="0":
                filename="output/sqldf_autosave_basic.csv"
            df_out = sqldf(df,sql,filename)
        else:
            print("table %s not exist!" %(table_name))
    def do_desc(self,line):
        """desc tables
desc [table_name]
    if table_name not exist, show table list
ex: desc table_def
        """
        pars=line.split()
        if len(pars)==0:
            print("table list:%s" %(gc.GAP.pd[self.domain_set[0]].keys()))
            return
        else:
            table_name = line
            if table_name in gc.GAP.pd[self.domain_set[0]].keys():
                df = gc.GAP.pd[self.domain_set[0]][table_name]
                print(df)
            else:
                print("table %s not exist!" %(table_name))

############ basin sub cmd ####################
class CliBasin(CliTblBase):
    def __init__(self,stdout):
        domain_set=['cli_basin','data/basin-河川流域範圍圖/basin-河川流域範圍圖.shp','NOGEOM',DOMAIN_SET_LOAD_GEO]
        CliTblBase.__init__(self,stdout,domain_set)

    def do_sql(self,line):
        """apply sql to select
sql sql_cmd
ex: sql "select * from df order by area desc"
        """
        sql = line.replace("\"","")

        df = pd.DataFrame(self.df.drop('geometry', axis=1))
        #df = self.df
        filename=''
        if gc.SETTING["SQLDF_AUTOSAVE"]!="0":
            filename="output/sqldf_autosave_basin.csv"
        df_out = sqldf(df,sql,filename)

############ river sub cmd ####################
class CliRiver(CliTblBase):
    def __init__(self,stdout):
        domain_set = ['cli_river','data/rivercode.csv','NOGEOM',DOMAIN_SET_LOAD_CSV]
        CliTblBase.__init__(self,stdout,domain_set)

    def do_sql(self,line):
        """apply sql to select
sql sql_cmd
ex: sql "select * from df where river_link like '0@130000%'"
        """
        sql = line.replace("\"","")
        df = self.df
        filename=''
        if gc.SETTING["SQLDF_AUTOSAVE"]!="0":
            filename="output/sqldf_autosave_river.csv"
        df_out = sqldf(df,sql,filename)



############ CliCx sub command ####################
class CliCx(cmd.Cmd):
    """
cx_dict={'basin_id':1300, 'basin_name':'頭前溪', 'geo':'data/basin-河川流域範圍圖/basin-河川流域範圍圖.shp',
    'dtm':'', 'ldd':''
}
"""
    def __init__(self,stdout):
        if stdout:
            cmd.Cmd.__init__(self,stdout=stdout)
        else:
            cmd.Cmd.__init__(self)

        self.basin_id='' #str

        self.cx_dicts = {}
        #self.cx_dicts['1300']={'basin_id':'1300', 'basin_name':'頭前溪', 'geo':'data/basin-河川流域範圍圖/basin-河川流域範圍圖.shp',
        #                    'dtm':'data/C1300_20m_elv0.tif', 'ldd':'data/C1300_20m_LDD.tif','load_type':CX_DICT_LOAD_SKIP}
        self.cx_dict=None
        self.df = None
        self.fd = None
        self.sto=9
        self.sto_range=[4,12] #default, will be override
    def set_basin(self,basin_id):
        self.basin_id = basin_id

        #load cx_dict
        filename="include/catchment.json"
        #cx_dicts = {}
        data = load_json(filename)
        self.cx_dicts = {}
        for i in range(len(data)):
            self.cx_dicts[data[i]['basin_id']]=data[i]
        #print("cx_dicts=%s" %(self.cx_dicts))

        if basin_id in self.cx_dicts.keys():
            self.load(self.cx_dicts[basin_id])
        else:
            print("no basin %s setting" %(basin_id))
            print("current basin supported: %s" %(self.cx_dicts.keys()))
    def load(self,cx_dict):
        #load geo and basin info
        self.cx_dict = cx_dict
        filename = cx_dict['geo']
        print("loading basin take few seconds, please wait!")
        df = geopandas.read_file(filename,encoding='utf-8')
        self.df = df[df['basin_no']==self.basin_id]
        if len(self.df.index)>0:
            filebasename="basin_c%s" %(self.basin_id)
            path_dir="output/%s" %(filebasename)
            if not os.path.exists(path_dir):
                os.mkdir(path_dir)
            dict_par={'encoding':'utf-8'}
            self.df.to_file("%s/%s.shp" %(path_dir,filebasename),**dict_par)
            print("%s shp saved: %s" %(filebasename, path_dir))

        #load flwdir
        dtm_file= cx_dict['dtm']
        flwdir_file=cx_dict['ldd']
        self.sto= cx_dict['min_sto']
        sto_range_str = cx_dict['sto_range']
        pars = sto_range_str.split(",")
        self.sto_range=[int(pars[0]),int(pars[1])]
        fd = FlwDir()
        fd.reload(dtm_file,flwdir_file)
        fd.init()
        filename = 'output/river_c%s_stream_%i.geojson' %(self.basin_id,self.sto)
        fd.streams(self.sto,filename)
        filename = 'output/river_c%s_subbas_%i.geojson' %(self.basin_id,self.sto)
        fd.subbasins_streamorder(self.sto,filename)
        self.fd = fd

    def do_set_basin(self,line):
        """set/load basin
set_basin [basin_id]
    if basin_id not supported, will display supported basin_id
ex: set_basin list
    set_basin 1300
        """
        pars=line.split()
        basin_id='1300'
        if len(pars)==1:
            basin_id = pars[0]
        self.set_basin(basin_id)
    def get_nearest_point_in_stream(self,point_src,dist_min=10000):
        #dist_min=10000
        idx_min=None
        point = None
        for index2, row2 in self.fd.gdf.iterrows():
            line = row2['geometry']
            dist = line.distance(point_src)
            if dist<dist_min:
                dist_min=dist
                idx_min=index2
        if idx_min:
            #print("point_id=%s%s,index=%i, minimal distance=%f" %(row['id'],row['name'],idx_min,dist_min))
            line_ori = self.fd.gdf.loc[idx_min]['geometry']

            pt_in = nearest_points(line_ori, point_src)[0]
            #print(pt_in.coords[0][0])
            xy=pt_in.coords[0]
            #point = [xy[0],xy[1],"%s|%s" %(row['id'],row['name'])]
            point = [xy[0],xy[1]]

        #else:
        #    print("index=%i, id=%s too far, > %i" %(index,row['id'],dist_min))
        return point
    def point_catchment_gen(self,csv_filename):
        sto=self.sto
        dist_min=10000
        filename = 'output/river_c%s_stream_%i.geojson' %(self.basin_id,sto)
        self.fd.streams(sto,filename)
        print("generating point_catchment by using stream(sto=%i)" %(sto))

        df= pd.read_csv(csv_filename)
        points=[]
        for index, row in df.iterrows():
        #source_note	source_email	id	name	twd97lon	twd97lat	twd97tm2x	twd97tm2y	note	process_note

            start = Point(row['twd97tm2x'], row['twd97tm2y'])
            #start = row['geom']
            point = self.get_nearest_point_in_stream(start)
            if point:
                point_name = [point[0],point[1],"%s|%s" %(row['id'],row['name'])]
                points.append(point_name)
            else:
                print("index=%i, id=%s too far, > %i" %(index,row['id'],dist_min))
        #print(points)
        for p in points:
            print("%s,%s,%s" %(p[0],p[1],p[2]))


        #points=[[260993,2735861,'油羅上坪匯流'],[253520,2743364,'隆恩堰'],[247785,2746443,'湳雅取水口']]
        self.fd.basins(points,'') #need 3826
        #self.fd.gdf_bas

    def do_output(self,line):
        """output different level of stream, subbas , point_catchment, downstream path ....
output [type] [...]
    type:
        stream : stream sto from 4-11,
        subbas : sto from 4-11,
        point_catchment_csv csv_filename : -need csv filename input or x,y for single point
        point_catchment x,y : - x,y for single point
        path x,y [x,y] [...] : generate downstream path
        pathline_interpolate [parts] [filename_csv] [filename_shp] : generate interpolate points from path, also output slope shape
        nx_write_shp : output network to shp
ex: output stream
    output subbas
    output point_catchment_csv "data/喝好水 吃好物 有良居-公民協力 - 點位集水區.csv"
    output point_catchment 253520,2743364
    output path 253520,2743364
    output nx_write_shp
    output pathline_interpolate 10
        """
        id = "stream"
        #filename = "data/喝好水 吃好物 有良居-公民協力 - 點位集水區.csv"
        if self.fd is None:
            return
        pars=line.split()
        if len(pars)>=1:
            id = pars[0]
        if id=="stream":

            for i in range(self.sto_range[0],self.sto_range[1]):
                filename = 'output/river_c%s_stream_%i.geojson' %(self.basin_id,i)
                self.fd.streams(i,filename)
        if id=="subbas":
            for i in range(self.sto_range[0],self.sto_range[1]):
                filename = 'output/river_c%s_subbas_%i.geojson' %(self.basin_id,i)
                self.fd.subbasins_streamorder(i,filename)
        if id =="point_catchment_csv":
            pars1=line.split("\"")
            if len(pars1)!=3:
                print("should follow the format")
                return
            csv_filename = path_sethome(pars1[1])
            if os.path.isfile(csv_filename):
                self.point_catchment_gen(csv_filename)
            else:
                print("CSV not exist!")
        if id =="point_catchment":
            if len(pars)>=2:
                xy_str=pars[1]
                xy = xy_str.split(",")

                sto=self.sto
                dist_min=10000
                filename = 'output/river_c%s_stream_%i.geojson' %(self.basin_id,sto)
                self.fd.streams(sto,filename)
                print("generating point_catchment by using stream(sto=%i)" %(sto))

                points=[]
                start = Point(float(xy[0]),float(xy[1]))
                point = self.get_nearest_point_in_stream(start)
                if point:
                    point_name = [point[0],point[1],"point_catchment"]
                    points.append(point_name)
                else:
                    print("(%s) too far, > %i" %(xy_str,dist_min))
                for p in points:
                    print("%s,%s,%s" %(p[0],p[1],p[2]))

                #points=[[260993,2735861,'油羅上坪匯流'],[253520,2743364,'隆恩堰'],[247785,2746443,'湳雅取水口']]
                self.fd.basins(points,'') #need 3826


        if id=="path":
            points=[]
            for i in range(1,len(pars)):
                xy_str=pars[i]
                xy = xy_str.split(",")
                points.append([float(xy[0]),float(xy[1])])
            if len(points)>0:
                self.fd.path(points,'')
            else:
                print("point data invalid!")
        if id =="nx_write_shp":
            self.fd.nx_write_shp()
        if id=="pathline_interpolate":
            parts=10
            gdf = None
            filename_csv = "output/pathline_height.csv"
            filename_shp = "output/pathline_slope.shp"
            if len(pars)>=2:
                parts = int(pars[1])
            if len(pars)>=3:
                filename_csv = pars[2]
            if len(pars)>=4:
                filename_shp = pars[3]
            if self.fd.gdf_paths is None:
                print("pathline need init! run output path first")
                return
            for index, row in self.fd.gdf_paths.iterrows():
                line_geo = row['geometry']
                gdf = self.fd.pathline_interpolate(line_geo,parts,filename_csv=filename_csv,filename_shp=filename_shp)
            if gdf is None:
                print("pathline need init! run output path first")

    def do_desc(self,line):
        """desc Cx"""
        if not self.df is None:
            print("basin_id=%s information:\n%s" %(self.basin_id,self.df))
            print("stream(%i):" %(self.sto))
            self.fd.desc_stream({'seg_info':1,'link_info':0,'dot_info':0})
        #desc network: nodes, edges and dot format
        self.fd.nx_desc()


    def do_info(self,line):
        """query information
info point_height x,y
     2node node_a node_b
     point_near x,y [min_distance] :  get stream nearest information by point, line_index, distance, point_x, point_y
     2point x1,y1 x2,y2 [min_distance] : desc 2point path, path length, river length

ex: info point_height 274202,2731387
    info 2node S0 E18
    info point_near 253520,2743364 5000
    info 2point 262572,2736940 247346,2747132 5000
        """
        if self.fd is None:
            return
        id="2node"
        pars=line.split()
        if len(pars)>=1:
            id = pars[0]

        if id=="2node":
            if len(pars)==3:
                start = pars[1]
                end = pars[2]
            else:
                return
            #path, path length
            path1 = self.fd.get_path(start,end)
            print("path %s->%s:%s" %(start,end,path1))
            print("edges %s->%s:%s" %(start,end,self.fd.path_get_edge(path1)))

            print("length information:")
            print("total length=%f" %(self.fd.path_length(path1)))
            kind = self.fd.nx_node_seq(start,end)
            print("kind %s->%s:%i (1: 順向 -1:逆向 0:沒在一條線)" %(start,end,kind))
        if id=="point_height":
            if len(pars)>=2:
                xy_str=pars[1]
            else:
                return
            xy = xy_str.split(",")
            height_src=self.fd.rio_value([float(xy[0]),float(xy[1])])
            print("src_x=%.3f,src_y=%.3f,height=%.3f" %(float(xy[0]),float(xy[1]),height_src))

        if id=="point_near":
            min_dist=5000
            xy_str=pars[1]
            if len(pars)>=3:
                min_dist = float(pars[2])
            xy = xy_str.split(",")
            height_src=self.fd.rio_value([float(xy[0]),float(xy[1])])
            print("src_x=%.3f,src_y=%.3f,height=%.3f" %(float(xy[0]),float(xy[1]),height_src))
            res = self.fd.point_with_streams([float(xy[0]),float(xy[1])],min_dist)
            if res:
                height=self.fd.rio_value([res[2],res[3]])
                print("line_index=%i, distance=%.3f, point_x=%.3f, point_y=%.3f,height=%.3f" %(res[0],res[1],res[2],res[3],height))
            else:
                print("distnace too far, min=%.3f" %(min_dist))
        if id=="2point":
            min_dist=5000
            if len(pars)>=4:
                xy1_str=pars[1]
                xy2_str=pars[2]
                min_dist = int(pars[3])
            else:
                return
            xy1 = xy1_str.split(",")
            xy2 = xy2_str.split(",")
            xys = [xy1,xy2]

            line_idxs=[]
            line_lengths=[]
            idx=0
            for xy in xys:
                print("Point(%s %s):" %(xy[0],xy[1]))
                res = self.fd.point_with_streams([float(xy[0]),float(xy[1])],min_dist)
                if res:
                    print("\tline_index=%i, distance=%.3f, point_x=%.3f, point_y=%.3f" %(res[0],res[1],res[2],res[3]))
                    [line_length,length_from_start]=self.fd.point_distance_in_line(res[0],[res[2],res[3]])
                    print("\tline_length=%f,length_from_start=%f" %(line_length,length_from_start))
                    line_idxs.append(res[0])
                    if idx==0:
                        line_lengths.append(line_length-length_from_start)
                    else:
                        line_lengths.append(length_from_start)

                else:
                    print("\tdistnace too far, min=%.3f" %(min_dist))
                idx+=1
            if len(line_lengths)!=2:
                return
            path = self.fd.get_edge_path(line_idxs[0],line_idxs[1])
            if path:
                path_point=path.copy()
                path_point.insert(1,'A')
                path_point.insert(len(path_point)-1,'B')
                print("path sequence:%s" %(path_point))
                print("edges :%s" %(self.fd.path_get_edge(path)))

                river_length = self.fd.path_length(path_point[2:-2]) +  line_lengths[0] + line_lengths[1]
                print("river length between A(%s),B(%s): %f" %(xy1_str,xy2_str,river_length))
            else:
                print("A(%s)->B(%s) don't have path!" %(xy1_str,xy2_str))

        #direction
        pass

    def do_quit(self, line):
        """quit this sub command"""
        """quit"""
        return True

############ sourcingwater sub cmd ####################
class CliSourcingWater(cmd.Cmd):
    def __init__(self,stdout):
        if stdout:
            cmd.Cmd.__init__(self,stdout=stdout)
        else:
            cmd.Cmd.__init__(self)
    def do_asks(self,line):
        """ask sourcingwater questions from json setting
asks [pathname]
ex: asks output/asks.json
"""

        pars=line.split()
        filename = "output/asks.json"
        if len(pars)>=1:
            filename = pars[0]
        asks_json = load_json(filename)
        print("----- asks by json -----\n%s\n----------" %(asks_json))
        sourcingwater_asks(asks_json)
    def do_quit(self, line):
        """quit this sub command"""
        """quit"""
        return True