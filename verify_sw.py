# @file verify_sw.py
# @brief simplest verification for SourcingWater environment
# @author wuulong@gmail.com
from codes.flwdir import *

def load_json_local(filename):
    data = None
    try:
        data_date = ""
        with open(filename , 'r', encoding='UTF-8') as json_file:
            data = json.load(json_file)
            return data
    except:
        print("%s:%s" %(filename,"EXCEPTION!"))
        return None

basin_id='1300'
sto=9
fd=None

#load cx_dict
filename="include/catchment.json"
data = load_json_local(filename)
cx_dicts = {}
for i in range(len(data)):
    cx_dicts[data[i]['basin_id']]=data[i]

cx_dict = cx_dicts['1300']
#load flwdir
dtm_file= cx_dict['dtm']
flwdir_file=cx_dict['ldd']
sto= cx_dict['min_sto']
fd = FlwDir()
fd.reload(dtm_file,flwdir_file)
fd.init()
filename = 'output/river_c%s_stream_%i.geojson' %(basin_id,sto)
fd.streams(sto,filename)
filename = 'output/river_c%s_subbas_%i.geojson' %(basin_id,sto)
fd.subbasins_streamorder(sto,filename)

# test path
points=[[260993,2735861,'油羅上坪匯流'],[253520,2743364,'隆恩堰'],[247785,2746443,'湳雅取水口']]
fd.path(points,'')
# test point's catchment
fd.basins(points,'') #need 3826