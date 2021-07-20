if 1: #flwdir test
    from codes.flwdir import *
    from shapely.geometry import *
    import geopandas as gpd

    fd = FlwDir()
    fd.reload()
    fd.init()
    if 1: # filename rule: None: return json, '' use default filename
        fd.streams(9,'')
        fd.desc_stream()
    if 1:
        points=[[260993,2735861,'油羅上坪匯流'],[253520,2743364,'隆恩堰'],[247785,2746443,'湳雅取水口']]
        bas_gdf = fd.basins(points,'')
        fd.path(points,'')

    if 1:
        for i in range(7,11):
            fd.subbasins_streamorder(i,'')
    if 1:
        wkt_str="MultiLineString ((255779.34444821099168621 2742184.59869130607694387, 255062.52472444207523949 2741882.12604631343856454, 254328.86074706495855935 2742279.99766481388360262))"
        fd.join_line(wkt_str) #modify gdf

    if 1:
        points=[[260993,2735861,'油羅上坪匯流'],[253520,2743364,'隆恩堰'],[247785,2746443,'湳雅取水口']]
        fd.basins(points,'')

        bas_df = fd.gdf_bas

        for index, row in bas_df.iterrows():
            if row['name']=='隆恩堰' and row['value']==1:

                a = row['geometry']
            if row['name']=='油羅上坪匯流' and row['value']==1:
                b = row['geometry']
        c=a.difference(b)

        gs = gpd.GeoSeries([c],crs=fd.crs)
        gs.to_file('output/basin_diff_2point.geojson', driver='GeoJSON')
