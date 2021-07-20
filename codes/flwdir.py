# @file flwdir.py
# @brief pack pyflwdir to FlwDir()
# @author wuulong@gmail.com
#extend
import rasterio
from rasterio import features
import pyflwdir
import geopandas as gpd

#畫圖要用的函示
import matplotlib
import matplotlib.pyplot as plt
from matplotlib import cm, colors
import cartopy.crs as ccrs
import numpy as np


from shapely import wkt
from shapely.ops import split
from shapely.geometry import *
from shapely.ops import nearest_points


np.random.seed(seed=101)
matplotlib.rcParams['savefig.bbox'] = 'tight'
matplotlib.rcParams['savefig.dpi'] = 256
plt.style.use('seaborn-whitegrid')



class FlwDir():
    def __init__(self):

        self.flwdir = None #
        self.transform = None #
        self.crs = None #
        self.latlon = None #

        self.elevtn = None #
        self.prof = None #

        self.flw = None # main flow dir object
        self.gdf = None # stream
        self.gdf_subbas = None # subbasins_streamorder

        self.gdf_paths = None # downstream path
        self.gdf_pnts =  None # downstream point

    def reload(self,dtm_file='data/C1300_20m_elv0.tif',flwdir_file='data/C1300_20m_LDD.tif'):

        with rasterio.open(flwdir_file, 'r') as src1:
            self.flwdir = src1.read(1)
            self.transform = src1.transform
            self.crs = src1.crs
            self.latlon = self.crs.to_epsg() == 4326
            print("%s info:%s" %(flwdir_file,src1))
        with rasterio.open(dtm_file, 'r') as src2:
            self.elevtn = src2.read(1)
            self.elevtn[self.elevtn==-99999]=-9999
            self.prof = src2.profile
            print("%s info:%s" %(dtm_file,src2))
    def init(self):
        ftype='ldd'
        self.flw = pyflwdir.from_array(self.flwdir,ftype=ftype, transform=self.transform, latlon=self.latlon, cache=True) #d8
        print(self.flw)
    def quickplot(self,gdfs=[], maps=[], hillshade=True, title='', filename='flw', save=False):

        fig = plt.figure(figsize=(8,15))
        ax = fig.add_subplot(projection=ccrs.PlateCarree())
        # plot hillshade background
        if hillshade:
            ls = matplotlib.colors.LightSource(azdeg=115, altdeg=45)
            hillshade = ls.hillshade(np.ma.masked_equal(self.elevtn, -9999), vert_exag=1e3)
            ax.imshow(hillshade, origin='upper', extent=self.flw.extent, cmap='Greys', alpha=0.3, zorder=0)
        # plot geopandas GeoDataFrame
        for gdf, kwargs in gdfs:
            gdf.plot(ax=ax, **kwargs)
        for data, nodata, kwargs in maps:
            ax.imshow(np.ma.masked_equal(data, nodata), origin='upper', extent=flw.extent, **kwargs)
        ax.set_aspect('equal')
        ax.set_title(title, fontsize='large')
        ax.text(0.01, 0.01, 'created with pyflwdir', transform=ax.transAxes, fontsize='large')
        if save:
            plt.savefig(f'output/{filename}.png')
        return ax

    # 函式
    def vectorize(self,data, nodata, transform, crs):

        feats_gen = features.shapes(
            data, mask=data!=nodata, transform=transform, connectivity=8,
        )
        feats = [
            {"geometry": geom, "properties": {"value": val}}
            for geom, val in list(feats_gen)
        ]

        # parse to geopandas for plotting / writing to file
        gdf = gpd.GeoDataFrame.from_features(feats, crs=self.crs)

        return gdf
    def upstream_area(self):
        # calculate upstream area
        self.uparea = self.flw.upstream_area(unit='km2')

    def streams(self, min_sto, filename=None): # None: return json, '' use default filename
        #河道
        feats = self.flw.streams(min_sto=9)
        self.gdf = gpd.GeoDataFrame.from_features(feats, crs=self.crs)

        if filename=='':
            filename = 'output/river_c1300_stream_%i.geojson' %(min_sto)

        if filename is None:
            return self.gdf.to_json()
        else:
            #print("filename=%s" %(filename))
            self.gdf.to_file(filename, driver='GeoJSON')

    def subbasins_streamorder(self,min_sto, filename=None):# None: return json, '' use default filename
        # calculate subbasins with a minimum stream order 7
        subbas = self.flw.subbasins_streamorder(min_sto=min_sto, mask=None)
        self.gdf_subbas = self.vectorize(subbas.astype(np.int32), 0, self.flw.transform,self.crs)

        if filename=='':
            filename = 'output/river_c1300_subbas_%i.geojson' %(min_sto)
        if filename is None:
            return self.gdf_subbas.to_json()
        else:
            self.gdf_subbas.to_file(filename, driver='GeoJSON')

    def path(self,points,filename=None): # None: return json, '' use default filename
        # 算通過點的下游路線
        # flow paths return the list of linear indices
        x=[]
        y=[]
        for p in points:
            x.append(p[0])
            y.append(p[1])
        xy = (x,y)
        #points=[[260993,2735861,'油羅上坪匯流'],[253520,2743364,'隆恩堰'],[247785,2746443,'湳雅取水口']]
        #xy=([260993, 253520, 247785], [2735861, 2743364, 2746443])

        if filename=='':
            filename = 'output/river_c1300_path.geojson'
        flowpaths, dists = self.flw.path(xy=xy, max_length=400e3, unit='m')
        # which we than use to vectorize to geofeatures
        feats = self.flw.geofeatures(flowpaths)
        self.gdf_paths = gpd.GeoDataFrame.from_features(feats, crs=self.crs).reset_index()
        self.gdf_pnts = gpd.GeoDataFrame(geometry=gpd.points_from_xy(*xy)).reset_index()
        if filename is None:
            return self.gdf_paths.to_json()
        else:
            self.gdf_paths.to_file(filename, driver='GeoJSON')

    def basins(self,points,filename=''): # None: return json, '' use default filename
        # 通過點的上游流域
        #points=[[260993,2735861,'油羅上坪匯流'],[253520,2743364,'隆恩堰'],[247785,2746443,'湳雅取水口']]

        nodata=0
        transform = self.flw.transform
        crs = self.crs
        featss = []
        for p in points:
            x, y = np.array([p[0], p[0]+250]), np.array([p[1], p[1]+250])
            name = p[2]
            subbasins = self.flw.basins(xy=(x,y), streams=self.flw.stream_order()>=4)
            gdf_bas = self.vectorize(subbasins.astype(np.int32), 0, self.flw.transform,self.crs)
            data = subbasins.astype(np.int32)
            feats_gen = features.shapes(
                data, mask=data!=nodata, transform=transform, connectivity=8,
            )
            feats = [
                {"geometry": geom, "properties": {"name": name, "value": val}}
                for geom, val in list(feats_gen)
            ]
            featss.extend(feats)

        self.gdf_bas = gpd.GeoDataFrame.from_features(featss, crs=crs)
        if filename=='':
            filename = 'output/river_c1300_basin.geojson'

        if filename is None:
            return self.gdf_bas.to_json()
        else:
            self.gdf_bas.to_file(filename, driver='GeoJSON')


    def desc_stream(self):
        # 觀察 stream 的連接性
        coords={}
        for index, row in self.gdf.iterrows():
            line = row['geometry']
            idxs=row['idxs']
            points = list(line.coords)
            start = points[0]
            end = points[len(points)-1]
            coords[index]=[start,end]
            seg_cnt = len(line.coords)
            print("index=%i,length=%i,seg cnt=%i,avg_len=%.1f,start=%s,end=%s,idxs=%i" %(index,line.length,seg_cnt,line.length/seg_cnt,start,end,idxs))

        link={}
        for key in coords.keys():
            start,end = coords[key]
            for key2 in coords.keys():
                start2,end2 = coords[key2]
                if end==start2:
                    if key in link:
                        print("index=%i already have start=%i" %(key,link[key]))
                    link[key]=key2
        print("link=%s" %(link))

        if 0:
            for i in range(len(gdf.index)):
                if i in link.keys():
                    #print("index=%i PASS" %(i))
                    pass
                else:
                    print("index=%i,info=%s" %(index,gdf.iloc[i]))

        for l in link:
            print("N%i->N%i" %(l,link[l]))
    def join_line(self,wkt_str):
        #圳路接入主流
        # line_ori:要修改的線, line_need:想加入的線, line_append:更新起點的想加入線, line_split:要修改的線被匯流點切開的結果
        #取得圳路 linestring

        #wkt_str="MultiLineString ((255779.34444821099168621 2742184.59869130607694387, 255062.52472444207523949 2741882.12604631343856454, 254328.86074706495855935 2742279.99766481388360262))"
        line_need = wkt.loads(wkt_str)
        print("line_need=%s" %(line_need))
        #經由圳路的起點，找到最接近需要修改的線
        start = Point(list(line_need.geoms[0].coords)[0])
        print("start:%s" %(start))
        dist_min=5000
        idx_min=None
        for index, row in self.gdf.iterrows():
            line = row['geometry']
            dist = line.distance(start)
            if dist<dist_min:
                dist_min=dist
                idx_min=index
        print("index=%i, minimal distance=%f" %(idx_min,dist_min))
        #找到匯流點
        line_ori = self.gdf.loc[idx_min]['geometry']
        pt_in = nearest_points(line_ori, start)[0]
        #修改圳路起點為匯流點
        pts = []
        pts.append(list(pt_in.coords)[0])
        for i in range(len(line_need.geoms)):
            pts.extend(list(line_need.geoms[i].coords))
        line_append = LineString(pts)
        print("line_append=%s" %(line_append))
        #取得主流切開後的兩線段
        line_split = split(line_ori, pt_in)
        print("line_split=%s"%(line_split))
        #在 gdf 中刪掉原線段，加入兩新線段,加入修改後的圳路
        idx_start = len(self.gdf.index)

        self.gdf.loc[idx_start]=[line_append,100,False,9] #geometry,idxs,pit,strord
        for i in range(len(line_split)):
            self.gdf.loc[idx_start+i+1]=[line_split[i],100+i+1,False,9]
        self.gdf.drop(idx_min,axis=0,inplace=True)
        self.gdf.to_file('output/river_c1300_mergeline.geojson', driver='GeoJSON')


