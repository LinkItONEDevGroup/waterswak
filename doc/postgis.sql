-- management
select * from pg_indexes;



DROP INDEX riverpoly_geom_idx;


CREATE INDEX riverpoly_geom_idx ON public.riverpoly USING gist (geom)
-- 水位測站
select st_no,name_c,ST_AsEWKT(geom) as ewkt from RIVWLSTA_e where tri_name in ('頭前溪','上坪溪','油羅溪');
select st_no,name_c,ST_AsEWKT(geom) as ewkt from RIVWLSTA_e where name_c='道將圳'

-- 河道
select * from riverpoly where river_name in ('頭前溪','上坪溪','油羅溪')

--水位測站跟河道距離
select river_name,ST_Distance(geom,ST_GeomFromText('POINT(251968.79 2744498.55)')) as dist
from riverpoly where river_name='頭前溪' order by dist limit  1

select S.st_no,S.name_c,S.tri_name,ST_Distance(S.geom,R.geom) as dist from RIVWLSTA_e as S
join riverpoly_rivercode as R
    on S.tri_name=R.river_name
where S.tri_name in ('頭前溪','上坪溪','油羅溪')
order by dist;


-- 水位測站的水庫集水區
select S.st_no, S.name_c, R.name from RIVWLSTA_e as S
join reservoir as R
    on ST_Within(S.geom,R.geom)
where name='隆恩堰';
    

-- 將 riverpoly 同河合併
DROP TABLE IF EXISTS riverpoly_rivercode;

CREATE TABLE riverpoly_rivercode AS
SELECT
  river_code,river_name,
  ST_Union(geom) AS geom
FROM riverpoly
GROUP BY river_code,river_name;

CREATE INDEX riverpoly_rivercode_geom_idx ON public.riverpoly_rivercode USING gist (geom);

-- 流域
select ST_AsGeoJson(geom) from basin where basin_name='頭前溪'

-- 所有測站位置的 view
/*
雨量站位置圖_水利署_現存站:ppobsta_wra_e
河川水位測站位置圖_現存站:RIVWLSTA_e
河川流量測站位置圖_現存站:    RIVQASTA_e
含沙量測站位置圖 RIVSESTA
近海水文潮位站位置圖    hyctide
近海水文氣象站位置圖    hycw
近海水文資料浮標站位置圖    hycbuoy
地下水觀測井位置圖_現存站    gwobwell_e
地層下陷GPS監測站    LASUBSTA
地下水水質監測站位置圖    gwqmosta
磁環分層式地層下陷監測井    subasta #id 欄位都是空的
      
公有單位抗旱井    DRWell
水門位置圖 DIKEGATE
抽水站位置圖    PUMP_DRAIN
水庫堰壩位置圖    SWRESOIR
*/
CREATE OR REPLACE VIEW sensor_station as
select '雨量站位置圖' as type, st_no as id,name_c as name,geom from ppobsta_wra_e
union
select '河川水位測站' as type,st_no as id,name_c as name,geom from RIVWLSTA_e
union
select '河川流量測站' as type,st_no as id,name_c as name,geom from RIVQASTA_e
union
select '含沙量測站' as type,st_no as id,name_c as name,geom from RIVSESTA
union
select '近海水文潮位站' as type,code as id,name,geom from hyctide
union
select '近海水文氣象站' as type,code as id,name,geom from hycw
union
select '近海水文資料浮標站' as type,code as id,name,geom from hycbuoy
union
select '地下水觀測井' as type,st_no as id,name_c as name,geom from gwobwell_e
union
select '地層下陷GPS監測站' as type,station_no as id,name_c as name,geom from LASUBSTA
union
select '地下水水質監測站' as type,well_no as id,name_c as name,geom from gwqmosta
union
select '磁環分層式地層下陷監測井' as type,name as id, name, geom from subasta
;

select * from sensor_station;

--
pgsql2shp -f output/sensor_station.shp -h localhost -p 5431 -u postgres -g geom postgis "select * from sensor_station;"


--
SELECT ST_LineLocatePoint('LINESTRING(0 0,2 2)','POINT(1.5 1.5)' );

ST_AsText(geom, 0)

select S.st_no,S.name_c,S.tri_name,ST_Distance(S.geom,R.geom) as dist from RIVWLSTA_e as S
join riverpoly_rivercode as R
    on S.tri_name=R.river_name
where S.tri_name in ('頭前溪')
order by dist;

select S.st_no,S.name_c,ST_LineLocatePoint(R.geom,S.geom) as locate from RIVWLSTA_e as S
join RIVERL as R
    on S.tri_name=R.rv_name
where S.tri_name in ('頭前溪') order by locate;

select * from riverpoly_rivercode where river_name='頭前溪';

select river_name, ST_Transform(ST_SetSRID(geom,3826), 4326) from riverpoly_rivercode where river_name='頭前溪';

select river_name, ST_Transform(ST_SetSRID(geom,3826), 4326) from RIVERL;

select rv_name, ST_Transform(ST_SetSRID(geom,3826), 4326) from RIVERL;

select  ST_GeometryType(geom) from RIVERL;

--
ogr2ogr -f "PostgreSQL" PG:"host=localhost port=5431 dbname=postgis user=postgres" "include/highway_test.geojson" -nln highway_test -append

ogr2ogr -f "PostgreSQL" PG:"host=localhost port=5431 dbname=postgis user=postgres" "include/C1300_trace_case1.geojsonl.json" -nln C1300 -append

-- 水位站跟道路的距離
select river_name,ST_Distance(geom,ST_GeomFromText('POINT(251968.79 2744498.55)')) as dist
from riverpoly where river_name='頭前溪' order by dist limit  1

select S.id,S.name,ST_Distance(S.geom,H.wkb_geometry) as dist from sensor_station as S
join highway_test as H
    on ST_DWithin(S.geom,H.wkb_geometry,10000)
where S.type='河川水位測站' and S.name='內灣'
order by dist

select * from sensor_station as S where S.type='河川水位測站' and S.name='內灣'

select S.st_no,S.name_c,S.tri_name,ST_Distance(S.geom,R.geom) as dist from RIVWLSTA_e as S
join riverpoly_rivercode as R
    on S.tri_name=R.river_name
where S.tri_name in ('頭前溪','上坪溪','油羅溪')
order by dist;

select "ref:zh",ST_Distance(wkb_geometry,'SRID=4326;POINT(121.182 24.704)') as dist from highway_test

select id,"ref:zh",ST_Distance(ST_Transform(wkb_geometry, 3826),'SRID=3826;POINT(268454 2733084)') as dist, ST_AsText(wkb_geometry) from highway_test order by dist limit 1

with target as (
    select id,"ref:zh",ST_Distance(ST_Transform(wkb_geometry, 3826),'SRID=3826;POINT(268454 2733084)') as dist, wkb_geometry  from highway_test order by dist limit 1
)
select S.type,S.name,ST_Transform(ST_ClosestPoint(ST_SetSRID(S.geom,3826),ST_Transform(target.wkb_geometry,3826)),4326)
from sensor_station as S, target
where S.type='河川水位測站' and S.name='內灣'


SELECT ST_AsEWKT(
  'SRID=3826;POINT(268454 2733084)'::geometry
);

SELECT ST_Transform(
  'SRID=3826;POINT(268454 2733084)'::geometry
,4326)
;

--
with target as (
select id,"ref:zh",ST_LineLocatePoint(ST_Transform(wkb_geometry,3826),'SRID=3826;POINT(268454 2733084)') as dist
from highway_test
)
select * from target
where dist>0 and dist<1
order by dist
;

SELECT UpdateGeometrySRID('riverpoly','geom',3826);

select * from geometry_columns where f_table_name='riverpoly';

with target as (
select * from c1300 limit 1
)
select S.name,MultiLineLocatePoint(ST_Transform(target.wkb_geometry,3826),ST_SetSRID(S.geom,3826)) as dist
from sensor_station as S, target
where S.type='河川水位測站' and S.name in ('舊港橋','內灣','經國橋','上坪','五峰大橋')
order by dist

#水位測站，在頭前溪的位置
with target as (
select * from c1300 limit 1
)
select S.name_c,MultiLineLocatePoint(ST_Transform(target.wkb_geometry,3826),ST_SetSRID(S.geom,3826)) as dist
from RIVWLSTA_e as S, target
where tri_name in ('頭前溪','上坪溪','油羅溪') and not name_c in('道將圳','頭前溪橋')
order by dist

select * from RIVWLSTA_e where tri_name='頭前溪' and not name_c='道將圳';
select *,ST_Transform(ST_SetSRID(geom,3826), 4326) from RIVWLSTA_e where name_c='頭前溪橋';

＃所有在頭前溪一公里內測站，在河內的位置
select S.type,S.name,ST_Distance(ST_SetSRID(S.geom,3826),ST_Transform(R.wkb_geometry,3826)) as dist, MultiLineLocatePoint(ST_Transform(R.wkb_geometry,3826),ST_SetSRID(S.geom,3826)) as loc
from sensor_station as S
join c1300 as R
    on ST_DWithin(ST_SetSRID(S.geom,3826),ST_Transform(R.wkb_geometry,3826),1000)
order by loc



select ST_Length(ST_Transform(wkb_geometry,3826)) from c1300 limit 1

select ST_Length(ST_LineSubstring(ST_Transform(wkb_geometry,3826),0,0.5)) from c1300 limit 1;

-- import raster
raster2pgsql -s 3826 -I -C -t 20x200 data/C1300_DTM_20M.tif public.c1300_dtm_20m > data/c1300_dtm_20m.sql

psql -h localhost -p 5431 -U postgres postgis -f data/c1300_dtm_20m.sql

SELECT rast
FROM c1300_dtm_20m
  WHERE ST_Intersects(rast, 'SRID=3826;POINT(268454 2733084)'::geometry);

SELECT rid,ST_Value(rast, 'SRID=3826;POINT(268454 2733084)'::geometry) as height
FROM c1300_dtm_20m
  WHERE ST_Intersects(rast, 'SRID=3826;POINT(268454 2733084)'::geometry);



