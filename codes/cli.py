# @file cli.py
# @brief CLI of whole tool
# @author wuulong@gmail.com
#standard
import cmd
import logging
#extend
import pandas as pd
import pandasql as ps
import geopandas
#library
import codes.globalclasses as gc
from codes.const import *
from codes.db import *
from codes.lib import *
from codes.riverlog import *
from codes.tools import *

from codes.flwdir import *
from shapely.geometry import *



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
    def do_status(self,line):
        """ show current status 
            status {desc_id}
            desc_id: 0-summary info, 1- detail info, 2- dot graph
            ex: status 1
        """
        pars=line.split()
        desc_id = 1
        if len(pars)==1:
            desc_id = pars[0]
        if desc_id=="connect":
            gc.GAP.conn=connect_db()
            if gc.GAP.conn is None:
                return "DB connection fail!"
            return ""
        #logging.info(gc.GAP.mm.desc(desc_id))

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

    def do_quit(self, line):
        """quit this sub command"""
        """quit"""
        return True

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
ex: set_basin 1300
        """
        pars=line.split()
        basin_id='1300'
        if len(pars)==1:
            basin_id = pars[0]
        self.set_basin(basin_id)

    def point_catchment_gen(self,csv_filename):
        sto=7
        filename = 'output/river_c%s_stream_%i.geojson' %(self.basin_id,sto)
        self.fd.streams(sto,filename)
        print("generating point_catchment by using stream(sto=%i)" %(sto))

        df= pd.read_csv(csv_filename)
        points=[]
        for index, row in df.iterrows():
        #source_note	source_email	id	name	twd97lon	twd97lat	twd97tm2x	twd97tm2y	note	process_note

            start = Point(row['twd97tm2x'], row['twd97tm2y'])
            #start = row['geom']

            dist_min=10000
            idx_min=None
            for index2, row2 in self.fd.gdf.iterrows():
                line = row2['geometry']
                dist = line.distance(start)
                if dist<dist_min:
                    dist_min=dist
                    idx_min=index2
            if idx_min:
                print("point_id=%s%s,index=%i, minimal distance=%f" %(row['id'],row['name'],idx_min,dist_min))
                line_ori = self.fd.gdf.loc[idx_min]['geometry']

                pt_in = nearest_points(line_ori, start)[0]
                #print(pt_in.coords[0][0])
                xy=pt_in.coords[0]
                point = [xy[0],xy[1],"%s|%s" %(row['id'],row['name'])]
                points.append(point)
            else:
                print("index=%i, id=%s too far, > %i" %(index,row['id'],dist_min))
        #print(points)
        for p in points:
            print("%s,%s,%s" %(p[0],p[1],p[2]))


        #points=[[260993,2735861,'油羅上坪匯流'],[253520,2743364,'隆恩堰'],[247785,2746443,'湳雅取水口']]
        self.fd.basins(points,'') #need 3826
        #self.fd.gdf_bas

    def do_output(self,line):
        """output different level of stream, subbas ....
output [type] [filename]
    type:
        stream : stream sto from 4-11,
        subbas : sto from 4-11,
        point_catchment csv_filename : -need csv filename input
        path x,y [x,y] [...] : generate downstream path
        point_near x,y [min_distance] :  get stream nearest information by point, line_index, distance, point_x, point_y
ex: output stream
    output point_catchment "data/喝好水 吃好物 有良居-公民協力 - 點位集水區.csv"
    output path 253520,2743364
    output point_near 253520,2743364 5000
        """
        id = "stream"
        #filename = "data/喝好水 吃好物 有良居-公民協力 - 點位集水區.csv"
        if self.fd is None:
            return
        pars=line.split()
        if len(pars)>=1:
            id = pars[0]
        if id=="stream":
            for i in range(4,12):
                filename = 'output/river_c%s_stream_%i.geojson' %(self.basin_id,i)
                self.fd.streams(i,filename)
        if id=="subbas":
            for i in range(4,12):
                filename = 'output/river_c%s_subbas_%i.geojson' %(self.basin_id,i)
                self.fd.subbasins_streamorder(i,filename)
        if id =="point_catchment":
            pars1=line.split("\"")
            if len(pars1)!=3:
                print("should follow the format")
                return
            csv_filename = path_sethome(pars1[1])
            if os.path.isfile(csv_filename):
                self.point_catchment_gen(csv_filename)
            else:
                print("%s not exist!" %(csv_filename))
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
        if id=="point_near":
            min_dist=5000
            xy_str=pars[1]
            if len(pars)>=3:
                min_dist = float(pars[2])
            xy = xy_str.split(",")
            res = self.fd.point_with_streams([float(xy[0]),float(xy[1])],min_dist)
            if res:
                print("line_index=%i, distance=%.3f, point_x=%.3f, point_y=%.3f" %(res[0],res[1],res[2],res[3]))
            else:
                print("distnace too far, min=%.3f" %(min_dist))
    def do_desc(self,line):
        """desc Cx"""
        if not self.df is None:
            print("basin_id=%s information:\n%s" %(self.basin_id,self.df))
            print("stream(%i):" %(self.sto))
            self.fd.desc_stream({'seg_info':1,'link_info':0,'dot_info':0})

    def do_quit(self, line):
        """quit this sub command"""
        """quit"""
        return True
