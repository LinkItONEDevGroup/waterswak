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

-- 建立所有測站位置的 view
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

-- 匯出 sensor_station 到 shp
pgsql2shp -f output/sensor_station.shp -h localhost -p 5431 -u postgres -g geom postgis "select * from sensor_station;"


-- 查 riverpoly_rivercode
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

-- 匯入 OSM highway 資料
ogr2ogr -f "PostgreSQL" PG:"host=localhost port=5431 dbname=postgis user=postgres" "include/highway_test.geojson" -nln highway_test -append

-- 匯入頭前溪線
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

-- highway_test 測試
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

-- sensor_station 內測站，在頭前溪的位置
with target as (
select * from c1300 limit 1
)
select S.name,MultiLineLocatePoint(ST_Transform(target.wkb_geometry,3826),ST_SetSRID(S.geom,3826)) as dist
from sensor_station as S, target
where S.type='河川水位測站' and S.name in ('舊港橋','內灣','經國橋','上坪','五峰大橋')
order by dist

-- 水位測站，在頭前溪的位置
with target as (
select * from c1300 limit 1
)
select S.name_c,MultiLineLocatePoint(ST_Transform(target.wkb_geometry,3826),ST_SetSRID(S.geom,3826)) as dist
from RIVWLSTA_e as S, target
where tri_name in ('頭前溪','上坪溪','油羅溪') and not name_c in('道將圳','頭前溪橋')
order by dist

select * from RIVWLSTA_e where tri_name='頭前溪' and not name_c='道將圳';
select *,ST_Transform(ST_SetSRID(geom,3826), 4326) from RIVWLSTA_e where name_c='頭前溪橋';

-- 所有在頭前溪一公里內測站，在河內的位置
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

-- 查測站高程
SELECT rid,ST_Value(rast, 'SRID=3826;POINT(268454 2733084)'::geometry) as height
FROM c1300_dtm_20m
  WHERE ST_Intersects(rast, 'SRID=3826;POINT(268454 2733084)'::geometry);
  
-- 查縣市鄉鎮村里
select countyid,countycode,countyname from county_moi;
select townid,towncode,countyname,townname from town_moi;
select villcode,countyname,townname,villname from village_moi_121;

#22 rows
select count(countyid) from county_moi;
-- 查測站縣市
SELECT AddGeometryColumn('county_moi','bbox','3824','GEOMETRY','2');
UPDATE county_moi SET bbox = ST_Envelope(ST_Force2D(geom));

#309ms
select S.type,S.name,C.countyname
from sensor_station as S
join county_moi as C
    on ST_Intersects(ST_Transform(ST_SetSRID(C.bbox,3824),3826),ST_SetSRID(S.geom,3826))
where S.type='河川水位測站'


SELECT AddGeometryColumn('town_moi','bbox','3824','GEOMETRY','2');
UPDATE town_moi SET bbox = ST_Envelope(ST_Force2D(geom));

#1145ms
select S.type,S.name,T.countyname,T.townname
from sensor_station as S
join town_moi as T
    on ST_Intersects(ST_Transform(ST_SetSRID(T.bbox,3824),3826),ST_SetSRID(S.geom,3826))
where S.type='河川水位測站'


SELECT AddGeometryColumn('village_moi_121','bbox','3826','GEOMETRY','2');
UPDATE village_moi_121 SET bbox = ST_Envelope(ST_Force2D(geom));

#2273ms
select S.type,S.name,V.countyname,V.townname,V.villname
from sensor_station as S
join village_moi_121 as V
    on ST_Intersects(V.bbox,ST_SetSRID(S.geom,3826))
where S.type='河川水位測站'
ERROR:  transform: latitude or longitude exceeded limits (-14)

#原來情況


#2min x second

select S.type,S.name,C.countyname
from sensor_station as S
join county_moi as C
    on ST_Intersects(ST_Transform(ST_SetSRID(C.geom,3824),3826),ST_SetSRID(S.geom,3826))
where S.type='河川水位測站'

#2min 37 second
select S.st_no,S.name_c,C.countyname
from RIVWLSTA_e as S
join county_moi as C
    on ST_Intersects(ST_Transform(ST_SetSRID(C.geom,3824),3826),ST_SetSRID(S.geom,3826))



st_no as id,name_c as name,geom from RIVWLSTA_e

#9min
select S.type,S.name,T.countyname,T.townname
from sensor_station as S
join town_moi as T
    on ST_Intersects(ST_Transform(ST_SetSRID(T.geom,3824),3826),ST_SetSRID(S.geom,3826))
where S.type='河川水位測站'

# 6033ms
select S.type,S.name,V.countyname,V.townname,V.villname
from sensor_station as S
join village_moi_121 as V
    on ST_Intersects(ST_SetSRID(V.geom,3826),ST_SetSRID(S.geom,3826))
where S.type='河川水位測站'

#設 geom column 出錯，查詢時的錯誤訊息
ERROR:  transform: latitude or longitude exceeded limits (-14)

select villcode,countyname,townname,villname from village_moi_121;


#286ms
select type,name
from sensor_station
where type='河川水位測站'

#281 rows
select count(name)
from sensor_station
where type='河川水位測站'

#58ms
select countyname from county_moi

#view 沒有 index, county_moi_geom_idx 有 index, RIVWLSTA_e 有 index
#換成RIVWLSTA_e有index 的也一樣，筆數都沒有很多

select countyname,ST_NPoints(geom) from county_moi

select countyname,ST_Subdivide(geom, 255) as geom_sub from county_moi

# 2min, 55s




with county_moi_sub as (
select countyname,ST_Subdivide(geom, 255) as geom_sub from county_moi
)
select countyname,ST_NPoints(geom_sub) from county_moi_sub

#沒用
SET enable_seqscan TO off;



# XY 最小值
SELECT countyname,townname,villname, ST_XMin(geom), ST_YMin(geom)
       FROM village_moi_121

--查地理欄位資訊
select * from geometry_columns where f_table_name='village_moi_121';

-- 淨水場水質表
CREATE TABLE m_waterwork_quality (
    "日期" date,
    "區處別" VARCHAR,
    "系統代號" VARCHAR,
    "淨水場名稱" VARCHAR,
    "項目" VARCHAR,
    "值" VARCHAR,
    "單位" VARCHAR,
    "飲用水水質標準" VARCHAR,
    "項次" VARCHAR,
    "淨水場資訊" VARCHAR
);

DELETE FROM m_waterwork_quality WHERE "日期" = '2021-05-01';


INSERT INTO m_waterwork_quality("日期", "區處別","系統代號","淨水場名稱","項目","值","單位","飲用水水質標準","項次","淨水場資訊")
VALUES ('2021-05-01', '一區', '101','新山淨水場','氟鹽(Fluoride)','0.04','mg/L','0.8','1','新山淨水場(Sinshan)基隆市麥金路720號[0162030]{0101}<基隆市@Keelung City>');

select distinct "日期" from m_waterwork_quality;
delete from

with tb as (
select distinct "項目" from m_waterwork_quality
)
select count("項目") from tb;

#'水質合格否(Y/N)'
select * from m_waterwork_quality
where "項目"='水質合格否(Y/N)' and "值"='N' ;

select * from m_waterwork_quality
where "淨水場名稱"='新竹第二淨水場';

select "項目","值"::DECIMAL from m_waterwork_quality
where "項目"='硝酸鹽氮(Nitrate)' ;

select "日期","淨水場名稱","項目","值"::DECIMAL as value from m_waterwork_quality
where "項目"='硝酸鹽氮(Nitrate)' and not "值" = 'ND' and "值"::DECIMAL>5.0
order by value desc;


select "日期","淨水場名稱","項目","值"::DECIMAL as value from m_waterwork_quality
where "項目"='硝酸鹽氮(Nitrate)' and isnumeric("值")
order by value desc;

select "日期","淨水場名稱","項目","值"::DECIMAL, "飲用水水質標準" from m_waterwork_quality
where "項目"='硝酸鹽氮(Nitrate)' and isnumeric("值")
order by "值" desc limit 5;


select "日期","淨水場名稱","項目","值"::DECIMAL as value from m_waterwork_quality
where isnumeric("值")
group by "項目"
order by value desc;


select "日期",EXTRACT(MONTH FROM "日期") as month,"淨水場名稱","項目","值"::DECIMAL as value from m_waterwork_quality
where "項目"='總溶解固體量(Total Dissolved Solids)' and "淨水場名稱" in ('新竹第一淨水場','新竹第二淨水場','東興淨水場','寶山淨水場','員崠淨水場','梅花淨水場','尖石淨水場','內灣淨水場','桃山淨水場','芎林淨水場')
order by value desc;



CREATE INDEX m_waterwork_quality_idx ON m_waterwork_quality ("日期");
CREATE INDEX m_waterwork_quality_idx2 ON m_waterwork_quality ("淨水場名稱");
CREATE INDEX m_waterwork_quality_idx3 ON m_waterwork_quality ("項目");

-- 匯入 台灣自來水公司供水轄區資訊
head -n 1000 data/台灣自來水公司供水轄區資訊.csv | /Volumes/F2020/opt/anaconda3/envs/py37/bin/csvsql -i postgresql --no-constraints --tables m_waterwork_area >> output/台灣自來水公司供水轄區資訊.sql
\copy "m_waterwork_area" FROM 'data/台灣自來水公司供水轄區資訊.csv' WITH (FORMAT csv,HEADER);


SELECT AddGeometryColumn('m_waterwork_area','geom','3826','POINT','2');
UPDATE m_waterwork_area SET geom = ST_Point("X座標(TWD97)", "Y座標(TWD97)");
CREATE INDEX m_waterwork_area_geom_idx ON public.m_waterwork_area USING gist (geom)


select ST_Point("X座標(TWD97)", "Y座標(TWD97)") from m_waterwork_area

select * from m_waterwork_area where "淨水場名稱" like '%第二%';
select * from m_waterwork_area where "淨水場名稱"='第二淨水場' and "區處別"=3;
區處別       | numeric           |           |          |
 淨水場名稱   | character varying |           |          |
 X座標(TWD97) | numeric           |           |          |
 Y座標(TWD97) | numeric           |           |          |
 主要供水轄區 | character varying |           |          |
 原水來源
 
 SELECT "淨水場名稱","原水來源",ST_MakePoint("X座標(TWD97)", "Y座標(TWD97)") as bbox from m_waterwork_area;

#頭前溪流域的淨水場
with waterwork as (
 SELECT "淨水場名稱","原水來源",ST_MakePoint("X座標(TWD97)", "Y座標(TWD97)") as bbox from m_waterwork_area
)
select W."淨水場名稱",C.basin_name, W."原水來源"
from waterwork as W
join basin as C
    on ST_Intersects(ST_SetSRID(W.bbox,3826),ST_SetSRID(C.geom,3826))
where C.basin_name='頭前溪'

select basin_no,basin_name from basin where basin_name='頭前溪';
select "淨水場名稱" from m_waterwork_area;

select * from geometry_columns where f_table_name='m_waterwork_area';

#頭前溪流域淨水場的水質
with waterwork_1300 as (
    with waterwork as (
     SELECT "淨水場名稱","原水來源",ST_MakePoint("X座標(TWD97)", "Y座標(TWD97)") as bbox from m_waterwork_area
    )
    select W."淨水場名稱",C.basin_name, W."原水來源"
    from waterwork as W
    join basin as C
        on ST_Intersects(ST_SetSRID(W.bbox,3826),ST_SetSRID(C.geom,3826))
    where C.basin_name='頭前溪'
)
select "日期",EXTRACT(MONTH FROM W."日期") as month,W."淨水場名稱","項目","值"::DECIMAL as value from m_waterwork_quality as W
join waterwork_1300 C
    on W."淨水場名稱" = C."淨水場名稱"
where "項目"='總溶解固體量(Total Dissolved Solids)' and W."淨水場名稱" in (select "淨水場名稱" from waterwork_1300)
order by value desc;


with waterwork_1300 as (
    with waterwork as (
     SELECT "淨水場名稱","原水來源",ST_MakePoint("X座標(TWD97)", "Y座標(TWD97)") as bbox from m_waterwork_area
    )
    select W."淨水場名稱",C.basin_name, W."原水來源"
    from waterwork as W
    join basin as C
        on ST_Intersects(ST_SetSRID(W.bbox,3826),ST_SetSRID(C.geom,3826))
    where C.basin_name='頭前溪'
)
select "日期",EXTRACT(MONTH FROM W."日期") as month,W."淨水場名稱","項目","值"::DECIMAL as value from m_waterwork_quality as W
join waterwork_1300 C
    on W."淨水場名稱" = C."淨水場名稱"
where "項目"='氨氮(Ammonia)' and isnumeric("值") and W."淨水場名稱" in (select "淨水場名稱" from waterwork_1300)
order by value desc;

-- 民眾到取水口水質/水量

select * from s_village_waterin
select * from s_waterin_quality
select * from m_waterwork_quality
select * from s_waterin_qty
select * from s_waterwork_qty

# 水源里，新竹第二淨水場，隆恩堰


select * from s_village_waterin where "VILLNAME"='水源里';
select * from s_waterin_quality where waterwork='第二淨水場';
select * from m_waterwork_quality where "淨水場名稱"='新竹第二淨水場';

#供水與原水相同的項目
汞(Mercury)
砷(Arsenic)
硒(Selenium)
鉛(Lead)
鉻(Chromium)
鎘(Cadmium)
氨氮(Ammonia)
大腸桿菌群(Coliform Group),大腸桿菌群(Coliform Group)





select distinct item from s_waterin_quality where waterwork='第二淨水場';


select * from s_waterin_quality where waterwork='第二淨水場' and item in ('汞','砷','硒','鉛','鉻','鎘','氨氮','大腸桿菌群');

select "日期","區處別","系統代號","淨水場名稱","項目","值","單位","飲用水水質標準" from m_waterwork_quality where "淨水場名稱"='新竹第二淨水場' and "項目" in ('汞(Mercury)','砷(Arsenic)','硒(Selenium)','鉛(Lead)','鉻(Chromium)','鎘(Cadmium)','氨氮(Ammonia)','大腸桿菌群(Coliform Group)','大腸桿菌群(Coliform Group)');

--河川水質監測站
select * from "e_river_station";
ALTER TABLE "河川水質監測站_121_10503" RENAME TO e_river_station;

#頭前溪流域的水質感測點
SELECT sitename,county,township,basin from e_river_station as S
join basin as C
    on ST_Intersects(ST_SetSRID(S.geom,3826),ST_SetSRID(C.geom,3826))
where C.basin_name='頭前溪'




-- EPA /wqx_p_01 河川水質監測資料 -> e_river_q
select count(*) from e_river_q;
select * from e_river_q where "SiteName"='中正大橋' and "ItemName"='氨氮'

select max("SampleDate") from e_river_q;

select distinct "River" from e_river_q where "Basin"='頭前溪流域';

select * from e_river_q where "SiteName"='內灣吊橋' order by "SampleDate";
select * from e_river_q where "SiteName"='中正大橋' and "ItemName"='氨氮' order by "SampleDate";

CAST ( "SampleDate" AS date )

select "SampleDate","ItemValue",extract(epoch from CAST ( "SampleDate" AS date )) as time from e_river_q where "SiteName"='中正大橋' and "ItemName"='氨氮' order by "SampleDate";

select "SampleDate","ItemValue",extract(epoch from CAST ( "SampleDate" AS date )) as time from e_river_q where "SiteName"='中正大橋' and "ItemName"='氨氮' and isnumeric("ItemValue") order by "SampleDate";


WITH upd AS (
    select "SampleDate","ItemValue",extract(epoch from CAST ( "SampleDate" AS date )) as time from e_river_q where "SiteName"='中正大橋' and "ItemName"='氨氮' and isnumeric("ItemValue") order by "SampleDate"
)
INSERT INTO t_time_series SELECT time,'river_q_中正大橋_氨氮',CAST ("ItemValue" as float) FROM upd;

select "SampleDate","ItemValue",extract(epoch from CAST ( "SampleDate" AS date )) as time from e_river_q where "SiteName"='中正大橋' and "ItemName"='氨氮' and not isnumeric("ItemValue") order by "SampleDate";

WITH upd AS (
    select "SampleDate","ItemValue",extract(epoch from CAST ( "SampleDate" AS date )) as time from e_river_q where "SiteName"='中正大橋' and "ItemName"='氨氮' and not isnumeric("ItemValue") order by "SampleDate"
)
INSERT INTO t_time_series SELECT time,'river_q_中正大橋_氨氮',0 FROM upd;

select * from t_time_series where id='river_q_中正大橋_氨氮' order by dt;


CREATE FUNCTION dup(in int, out f1 int, out f2 text)
    AS $$ SELECT $1, CAST($1 AS text) || ' is text' $$
    LANGUAGE SQL;

SELECT * FROM dup(42);

CREATE or replace FUNCTION iIF(
    condition boolean,       -- IF condition
    true_result anyelement,  -- THEN
    false_result anyelement  -- ELSE
) RETURNS anyelement AS $f$
  SELECT CASE WHEN condition THEN true_result ELSE false_result END
$f$  LANGUAGE SQL IMMUTABLE;


with r as (
select "SampleDate","ItemValue",extract(epoch from CAST ( "SampleDate" AS date )) as time,
    cast(iif(cast(isnumeric("ItemValue") as boolean ),"ItemValue",'0') as float) as value
 from e_river_q where "SiteName"='中正大橋' and "ItemName"='氨氮'  order by "SampleDate"
 )
 select time,value from r where time<1614729600 and time>1538524800

select "SampleDate","ItemValue",extract(epoch from CAST ( "SampleDate" AS date )) as time,
    cast(iif(cast(isnumeric("ItemValue") as boolean ),"ItemValue",'0') as float) as value
 from e_river_q where "SiteName"=${query0} and "ItemName"='氨氮'  order by "SampleDate"

內灣吊橋,寶山水庫取水口

select distinct "ItemName" from e_river_q
select distinct "SiteName" from e_river_q where "Basin"='淡水河流域'
select "SiteName","ItemValue","SampleDate" from e_river_q where "Basin"='淡水河流域' and "ItemName"='河川污染分類指標' order by "SampleDate"

select "SiteName","ItemValue","SampleDate" from e_river_q where "Basin"='淡水河流域' and "ItemName"='河川污染分類指標' order by "ItemValue" desc;

select distinct "SiteName" from e_river_q where "Basin"='淡水河流域' and "ItemName"='河川污染分類指標' and "ItemValue">'5';


 

$__unixEpochFilter(time)

--EPA //dws_p_28 每月自來水水質監測資料 -> e_waterwork_q
select count(*) from e_waterwork_q;
select * from e_waterwork_q limit 10;
select distinct "PLANT" from e_waterwork_q where "PLANT" like '%新竹%';
select distinct "ITEM" from e_waterwork_q;
select * from e_waterwork_q where "PLANT"='新竹給水廠新竹第一淨水場' and "ITEM"='氨氮' order by "CKDATE";
select * from e_waterwork_q where "ITEM"='合格' and not "ITEMVAL"='1' order by "CKDATE";

# 頭前溪流域水質測站，某個月的所有河川水質感測值
with river_station as (
    SELECT sitename,county,township,basin from e_river_station as S
    join basin as C
        on ST_Intersects(ST_SetSRID(S.geom,3826),ST_SetSRID(C.geom,3826))
    where C.basin_name='頭前溪'

)
select * from e_river_q as R where "SiteName" in (select sitename from river_station) and "ItemName"='氨氮' order by "SampleDate";

select * from e_river_q where "SiteName" ='中正大橋' and "ItemName"='氨氮' order by "SampleDate";


-- could not read block
postgis=# select * from e_river_q where "SiteName"='中正大橋' and "ItemName"='氨氮';
ERROR:  could not read block 79 in file "base/16386/46479": Bad address

select pg_relation_filepath('e_river_q');

postgis=# select pg_relation_filepath('e_river_q');
 pg_relation_filepath
----------------------
 base/16386/46479
 
 postgis=# \dt+ e_river_q
                             List of relations
 Schema |   Name    | Type  |  Owner   | Persistence | Size  | Description
--------+-----------+-------+----------+-------------+-------+-------------
 public | e_river_q | table | postgres | permanent   | 72 MB |
 
 $ pg_controldata -D "/Users/wuulong/Library/Application Support/Postgres/var-13" | grep checksum
Data page checksum version:           0

$ pg_checksums -e -P -D "/Users/wuulong/Library/Application Support/Postgres/var-13"
559/559 MB (100%) computed
Checksum operation completed
Files scanned:  2651
Blocks scanned: 71587
pg_checksums: syncing data directory
pg_checksums: updating control file
Checksums enabled in cluster

select * from e_river_q where "SiteName"='中正大橋' and "ItemName"='氨氮';
正常了，覺得應該只是要重開 server 即可

$ pg_controldata -D "/Users/wuulong/Library/Application Support/Postgres/var-13" | grep checksum
Data page checksum version:           1

pg_checksums -D "/Users/wuulong/Library/Application Support/Postgres/var-13" –check

$ pg_checksums -c -D "/Users/wuulong/Library/Application Support/Postgres/var-13"
Checksum operation completed
Files scanned:  2651
Blocks scanned: 71587
Bad checksums:  0
Data checksum version: 1

-- /wqx_p_12 河川水質季監測資料
select count(*) from e_river_season_q;
select * from e_river_season_q limit 10;
select max("SampleDate") from e_river_season_q;

with t1 as(
select *,EXTRACT(MONTH FROM CAST ( "SampleDate" AS date )) as month,EXTRACT(YEAR FROM CAST ( "SampleDate" AS date )) as year from e_river_season_q where "Basin"='頭前溪流域' and "ItemName"='氨氮'
)
select * from t1 where year=2021 and month=5 order by "SampleDate";

#頭前溪最新這個月的某測項值
with t1 as(
select *,EXTRACT(MONTH FROM CAST ( "SampleDate" AS date )) as month,EXTRACT(YEAR FROM CAST ( "SampleDate" AS date )) as year from e_river_season_q where "Basin"='頭前溪流域' and "ItemName"='氨氮'
)
select distinct on ("SampleDate") * from t1 where year=2021 and month=5 order by "SampleDate";

--/wqx_p_51 新竹縣水量水質自動監測連線傳輸監測紀錄值即時資料集
delete from e_cwms_hsinchu;
select * from e_cwms_hsinchu;

select *,CAST( "M_VAL" as DOUBLE PRECISION) as value from e_cwms_hsinchu
where "DESP"='導電度' order by value;

--m_6569-登記工廠名錄
select count(*) from "m_6569-登記工廠名錄";

     Column      |       Type       | Collation | Nullable | Default
------------------+------------------+-----------+----------+---------
 index            | bigint           |           |          |
 工廠名稱         | text             |           |          |
 工廠登記編號     | text             |           |          |
 工廠設立許可案號 | text             |           |          |
 工廠地址         | text             |           |          |
 工廠市鎮鄉村里   | text             |           |          |
 工廠負責人姓名   | text             |           |          |
 統一編號         | double precision |           |          |
 工廠組織型態     | text             |           |          |
 工廠設立核准日期 | double precision |           |          |
 工廠登記核准日期 | double precision |           |          |
 工廠登記狀態     | text             |           |          |
 產業類別         | text             |           |          |
 主要產品         | text             |           |          |
Indexes:

#8/11
select * from "m_6569-登記工廠名錄"
where 工廠名稱 in (select distinct "ABBR" from e_cwms_hsinchu );

select * from "m_6569-登記工廠名錄" limit 10;



select C."ABBR",C."DESP",C."M_DATE",C."M_TIME",C."M_VAL',C."STATUS",C."UNIT",CAST( "M_VAL" as DOUBLE PRECISION) as value,M."產業類別" from e_cwms_hsinchu C
join "m_6569-登記工廠名錄" M
    on C."ABBR"=M."工廠名稱"
where "DESP"='導電度' order by value;

#排廢水感測資料串上工廠產業別與主要產品
select C.*,CAST( "M_VAL" as DOUBLE PRECISION) as value,M."產業類別",M."主要產品" from e_cwms_hsinchu C
left join "m_6569-登記工廠名錄" M
    on C."ABBR"=M."工廠名稱"
where "DESP"='導電度' order by value


\copy ( select C.*,CAST( "M_VAL" as DOUBLE PRECISION) as value,M."產業類別",M."主要產品" from e_cwms_hsinchu C left join "m_6569-登記工廠名錄" M on C."ABBR"=M."工廠名稱" where "DESP"='導電度' order by value) To '/tmp/test.csv' With CSV HEADER


263印刷電路板、269其他電子零組件
263印刷電路板、199未分類其他化學製品
254金屬加工處理、261半導體
159其他紙製品、239其他非金屬礦物製品

-- #/ems_s_01 環境保護許可管理系統(暨解除列管)對象基本資料 = 118447-環境保護許可管理系統(暨解除列管)對象基本資料
labels={'EmsNo': '管制編號', 'FacilityName': '事業名稱', 'UniformNo': '營利事業統一編號', 'County': '縣市', 'Township': '鄉鎮市區', 'FacilityAddress': '實際廠（場）地址', 'IndustryAreaName': '所在工業區名稱', 'IndustryID': '行業別代碼', 'IndustryName': '行業別名稱', 'TWD97TM2X': '二度分帶X座標（TWD97）', 'TWD97TM2Y': '二度分帶Y座標（TWD97）', 'WGS84Lon': '經度（WGS84）', 'WGS84Lat': '緯度（WGS84）', 'IsAir': '是否空列管', 'IsWater': '是否水列管', 'IsWaste': '是否廢棄物列管', 'IsToxic': '是否毒化物列管', 'IsSoil': '是否土壤列管', 'AirReleaseDate': '空解列日期', 'WaterReleaseDate': '水解列日期', 'WasteReleaseDate': '廢解列日期', 'ToxicReleaseDate': '毒解列日期', 'SoilReleaseDate': '土解列日期'}

postgis=# \d e_factory_base
               Table "public.e_factory_base"
      Column      |  Type  | Collation | Nullable | Default
------------------+--------+-----------+----------+---------
 index            | bigint |           |          |
 EmsNo            | text   |           |          |
 FacilityName     | text   |           |          |
 UniformNo        | text   |           |          |
 County           | text   |           |          |
 Township         | text   |           |          |
 FacilityAddress  | text   |           |          |
 IndustryAreaName | text   |           |          |
 IndustryID       | text   |           |          |
 IndustryName     | text   |           |          |
 TWD97TM2X        | text   |           |          |
 TWD97TM2Y        | text   |           |          |
 WGS84Lon         | text   |           |          |
 WGS84Lat         | text   |           |          |
 IsAir            | text   |           |          |
 IsWater          | text   |           |          |
 IsWaste          | text   |           |          |
 IsToxic          | text   |           |          |
 IsSoil           | text   |           |          |
 AirReleaseDate   | text   |           |          |
 WaterReleaseDate | text   |           |          |
 WasteReleaseDate | text   |           |          |
 ToxicReleaseDate | text   |           |          |
 SoilReleaseDate  | text   |           |          |
Indexes:
    "ix_e_factory_base_index" btree (index)


select count(*) from e_factory_base;
select * from e_factory_base limit 10;
select * from e_factory_base where "TWD97TM2Y"='0'

ALTER TABLE e_factory_base ADD COLUMN geom geometry(POINT, 3826);

UPDATE e_factory_base
SET geom = ST_SetSRID(ST_MakePoint(CAST ("TWD97TM2X" AS float8),CAST ("TWD97TM2Y" AS float8)),3826)::geometry;

CREATE INDEX e_factory_base_geom_idx ON public.e_factory_base USING gist (geom)
                
select * from geometry_columns where f_table_name='e_factory_base';

with t1 as (
select "TWD97TM2Y",isnumeric("TWD97TM2Y") as ck from e_factory_base
)
select * from t1 where not ck='t';

UPDATE e_factory_base
SET "TWD97TM2Y" = '0' where isnumeric("TWD97TM2Y")='f';


CAST ("TWD97TM2X" AS INTEGER)

--25598-台灣各工業區範圍圖資料集
select * from "25598-台灣各工業區範圍圖資料集";

-- 8818-工業區污水處理廠分布位置圖
shp2pgsql -W UTF-8 -D -s 102443 -I "./data/8818-工業區污水處理廠分布位置圖/工業區污水處理廠分布_121.shp" "8818-工業區污水處理廠分布位置圖">> data/shp.sql

select * from "8818-工業區污水處理廠分布位置圖";
select distinct "工業區名稱" from "8818-工業區污水處理廠分布位置圖"
select distinct "所在工業區" from "8818-工業區污水處理廠分布位置圖" where "所在工業區" like '%新竹%'

-- /stat_p_45 垃圾清理量資料 "total": 902 -> e_trash_stat_qty
labels={'year': '年度', 'month': '月份', 'county': '縣市別', 'GarbageGenerated': '垃圾產生量', 'GarbageClearance': '垃圾清運量', 'GarbageRecycled': '資源回收量', 'FoodWastesRecycled': '廚餘回收量', 'BulkWasteRecyclingandReuse': '巨大垃圾回收再利用量'}
total=902

select * from e_trash_stat_qty limit 10;
select * from e_trash_stat_qty where county='新竹市' order by year,CAST ( "month" AS INTEGER );
\copy ( select * from e_trash_stat_qty where county='新竹市' order by year,CAST ( "month" AS INTEGER )) To '~/Downloads/test.csv' With CSV HEADER

--/stat_p_46 垃圾清理回收率指標資料 "total": 2194 -> e_trash_recycle
labels={'Year': '年度', 'Month': '月份', 'County': '縣市別', 'Garbage_Recycled': '資源回收率', 'Food_Waste': '廚餘回收率', 'Bulk_Waste': '巨大垃圾回收再利用率', 'Municipal_Waste_Recycling_Rate': '垃圾回收率'}
total=2194

select count(*) from e_trash_recycle;
select * from e_trash_recycle limit 10;
select * from e_trash_recycle where "County"='新竹市' order by "Year",CAST ( "Month" AS INTEGER );

select distinct "County","Year","Month","Garbage_Recycled" from e_trash_recycle where "County"='新竹市' order by "Year","Month";

\copy ( select distinct "County","Year","Month","Garbage_Recycled" from e_trash_recycle where "County"='新竹市' order by "Year","Month") To '~/Downloads/test.csv' With CSV HEADER

-- /stat_p_87 垃圾處理場(廠)座數 "total": 575 -> e_garbage_disposal
'ItemName': '統計項名稱', 'Category': '類別', 'Year': '年度', 'ItemUnit': '單位', 'ItemValue': '數值'

select * from e_garbage_disposal limit 10;
select * from e_garbage_disposal where "Category"='新竹市' order by "Year"

-- 更新 table 的 geom srid
SELECT UpdateGeometrySRID('riverpoly_rivercode','geom',3826);
select * from geometry_columns where f_table_name='riverpoly_1300';

# view 沒有辦法更新 srid
select * from geometry_columns where f_table_name='sensor_station';


select river_name, ST_Transform(ST_SetSRID(geom,3826), 4326) from riverpoly_rivercode where river_name='頭前溪';

select river_name, ST_Transform(geom, 4326) from riverpoly_rivercode where river_name='頭前溪';

SELECT AddGeometryColumn('county_moi','bbox','3824','GEOMETRY','2');
text AddGeometryColumn(varchar table_name, varchar column_name, integer srid, varchar type, integer dimension, boolean use_typmod=true);

ppobsta_wra_e
RIVERL
RIVWLSTA_e
RIVQASTA_e
RIVSESTA
hyctide
hycw
hycbuoy
gwobwell_e
LASUBSTA
gwqmosta
subasta

OK SELECT UpdateGeometrySRID('ppobsta_wra_e','geom',3826);
select * from geometry_columns where f_table_name='ppobsta_wra_e';

-- 109年6月新竹市統計區人口統計_最小統計區_SHP
select * from "109年6月新竹縣統計區人口統計_最小統計區" limit 3;
select * from "109年6月新竹市統計區人口統計_最小統計區" limit 3;
select * from "109年6月新竹縣統計區人口指標_最小統計區" limit 3;
select * from "109年6月新竹市統計區人口指標_最小統計區" limit 3;

-- 6932-自來水用水量
select * from "6932-自來水用水量";

 Table "public.6932-自來水用水量"
                   Column                    |       Type       | Collation | Nullable | Default
---------------------------------------------+------------------+-----------+----------+---------
 index                                       | bigint           |           |          |
 County                                      | text             |           |          |
 Month                                       | bigint           |           |          |
 TheDailyDomesticConsumptionOfWaterPerPerson | double precision |           |          |
 Town                                        | text             |           |          |
 Year                                        | bigint           |           |          |

select * from "6932-自來水用水量" where "County"='新竹市' order by "Year","Month"::INTEGER;
select "Month",avg("TheDailyDomesticConsumptionOfWaterPerPerson") from "6932-自來水用水量" where "County"='新竹市' group by "Month" order by  "Month"::INTEGER;

\copy ( select * from "6932-自來水用水量" where "County"='新竹市' order by "Year","Month"::INTEGER) To '/tmp/test.csv' With CSV HEADER

\copy ( select "Month",avg("TheDailyDomesticConsumptionOfWaterPerPerson") from "6932-自來水用水量" where "County"='新竹市' group by "Month" order by  "Month"::INTEGER) To '~/Downloads/water.csv' With CSV HEADER

--
Create a read-only user in PostgreSQL
1. To create a new user in PostgreSQL:
CREATE USER lass WITH PASSWORD 'lass123456';

2. GRANT the CONNECT access:
GRANT CONNECT ON DATABASE postgis TO lass;

3. Then GRANT USAGE on schema:
GRANT USAGE ON SCHEMA public TO lass;

4. GRANT SELECT
Grant SELECT for a specific table:
GRANT SELECT ON table_name TO username;
Grant SELECT for multiple tables:
GRANT SELECT ON ALL TABLES IN SCHEMA public TO lass;
If you want to grant access to the new table in the future automatically, you have to alter default:
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO lass;

-- test time series data
CREATE TABLE t_time_series (
    dt bigint,
    id VARCHAR,
    value float
);

INSERT INTO t_test1 VALUES (1,'test1', 2.3);

delete from t_time_series where dt=1625821800;
INSERT INTO t_time_series VALUES (1625821800,'riverlog_rain_hourdiff_station_cnt', 2.00);

select * from t_time_series where dt=1625821800;

drop table t_test1;

-- e_river_state_q
select * from e_river_state_q;

      Column      |  Type  | Collation | Nullable | Default
------------------+--------+-----------+----------+---------
 index            | bigint |           |          |
 統計期           | text   |           |          |
 統計區           | text   |           |          |
 監測站數         | text   |           |          |
 溶氧量平均值     | text   |           |          |
 生化需氧量平均值 | text   |           |          |
 懸浮固體平均值   | text   |           |          |
 氨氮平均值       | text   |           |          |
 溶氧量最大值     | text   |           |          |
 生化需氧量最大值 | text   |           |          |
 懸浮固體最大值   | text   |           |          |
 氨氮最大值       | text   |           |          |

select * from e_river_state_q where "統計區"='頭前溪' order by "統計期"

--
select table_name,table_type from information_schema.tables where table_schema ='public' and table_type='BASE TABLE' order by table_name

\copy ( select table_name,table_type from information_schema.tables where table_schema ='public' and table_type='BASE TABLE' order by table_name) To '~/Downloads/debug.csv' With CSV HEADER

--information_schema.routines
select * from information_schema.routines;


\copy ( select * from information_schema.routines) To '~/Downloads/debug.csv' With CSV HEADER


-- desc A1
select max("CKDATE") from e_waterwork_q;
select DISTINCT "ITEM","CKDATE","ITEMVAL" from e_waterwork_q where "PLANT"='新竹給水廠新竹第二淨水場' and "TOWNSHIP"='東區' and "CKDATE" like '110.01.%'  order by "CKDATE";

select "ITEM",CAST("CKDATE" as date),"ITEMVAL" from e_waterwork_q where "PLANT"='新竹給水廠新竹第二淨水場' and "TOWNSHIP"='東區' and "CKDATE">= '110.01.01' and "CKDATE"< '110.02.01' order by "CKDATE";


select * from e_waterwork_q where "ITEM"='合格' and not "ITEMVAL"='1' order by "CKDATE";

select EXTRACT(YEAR FROM max(date)) as year,EXTRACT(MONTH FROM max(date)) as month from s_waterwork_qty where waterwork='寶山淨水廠'

select max(date) from s_waterwork_qty where waterwork='寶山淨水廠'
select * from s_waterwork_qty where waterwork='寶山淨水廠' and date >='2021-5-1' and date < '2021-6-1'

select * from m_waterwork_quality where '淨水場名稱'='新竹第二淨水場';

select "日期","區處別","系統代號","淨水場名稱","項目","值","單位","飲用水水質標準" from m_waterwork_quality where "淨水場名稱"='新竹第二淨水場' and "項目" in ('汞(Mercury)','砷(Arsenic)','硒(Selenium)','鉛(Lead)','鉻(Chromium)','鎘(Cadmium)','氨氮(Ammonia)','大腸桿菌群(Coliform Group)','大腸桿菌群(Coliform Group)');


select * from s_waterin_qty where waterin='隆恩堰' order by date;
select max(date) from s_waterin_qty where waterin='隆恩堰'
select * from s_waterin_qty where waterin='隆恩堰' and date >='2021-5-1' and date < '2021-6-1'

waterin |     date      |  qty
---------+---------------+--------
 隆恩堰  | 0110年4月1日  | 127744

select max(date) as date from s_waterin_quality; #2021-03-22
select * from s_waterin_quality where waterwork='第二淨水場' and date >='2021-3-1' and date < '2021-4-1';
 
    date    | waterwork  |      item       |  value  | limit
------------+------------+-----------------+---------+-------
 2021-03-22 | 第二淨水場 | 大腸桿菌群      | 2800    | 20000

-- s_waterin_b
# add geom field, after first import
SELECT AddGeometryColumn('s_waterin_b','geom','3826','POINT','2');
UPDATE s_waterin_b SET geom = ST_PointFromText(wkt_geom);
CREATE INDEX s_waterin_b_geom_idx ON public.s_waterin_b USING gist (geom)


select * from s_waterin_b where name='頭前溪(隆恩堰）';
select ST_AsText(ST_Transform(geom,4326)) from s_waterin_b where name='頭前溪(隆恩堰）';

select river_name, ST_Transform(ST_SetSRID(geom,3826), 4326) from riverpoly_rivercode where river_name='頭前溪';


--
select * from "b_水門";


-- #頭前溪流域內的水質測站
select * from basin where basin_name='頭前溪'
select * from e_river_station


select * from geometry_columns where f_table_name='e_river_station';

SELECT UpdateGeometrySRID('e_river_station','geom',3826);
select * from geometry_columns where f_table_name='ppobsta_wra_e';


-- schema 測試
CREATE SCHEMA hackathon;

CREATE TABLE hackathon.t_test1 (
    dt bigint,
    id VARCHAR,
    value float
);

ALTER TABLE t_test SET SCHEMA hackathon;

#CREATE SCHEMA hackathon AUTHORIZATION team_test

CREATE ROLE team with LOGIN
GRANT USAGE ON SCHEMA hackathon TO team;


createuser -D team_mgr -g team -P -R -p 5431 -U postgres

psql -h localhost -p 5431 -U team_mgr postgis -W

ALTER USER postgres PASSWORD 'myPassword';

ALTER TABLE e_waterwork_q SET SCHEMA hackathon;
e_cwms_hsinchu
e_factory_base
e_garbage_disposal
e_river_q
e_river_season_q
e_river_state_q
e_trash_recycle
e_trash_stat_qty
e_waterwork_q

SET search_path TO "$user",hackathon,public;
ALTER ROLE team SET search_path TO "$user",hackathon,public;
ALTER USER team_mgr SET search_path TO "$user",hackathon,public;

ALTER USER team_test RENAME TO team_mgr;

select * from e_river_q;

GRANT SELECT, UPDATE, INSERT, DELETE, TRUNCATE ON
ALL TABLES IN SCHEMA hackathon TO team;

GRANT SELECT, UPDATE, INSERT, DELETE, TRUNCATE ON
ALL TABLES IN SCHEMA public TO team;

-- 查 GPS 鄉鎮
select townname from town_moi where ST_Intersects('SRID=3826;POINT(268313 2733131)'::geometry,ST_Transform(ST_SetSRID(geom,3824),3826))

select townname,villname from village_moi_121 where ST_Intersects('SRID=3826;POINT(268313 2733131)'::geometry,ST_SetSRID(geom,3826))

--22371-工業區污水處理廠放流水水質資
select * from "22371-工業區污水處理廠放流水水質資訊" where "日期">20210101;
select max("日期") from "22371-工業區污水處理廠放流水水質資訊"
select distinct "廠名" from "22371-工業區污水處理廠放流水水質資訊";
select distinct "日期" from "22371-工業區污水處理廠放流水水質資訊" where "日期">20210301 and "廠名"='新竹' ;
select * from "22371-工業區污水處理廠放流水水質資訊" where "日期">20210301 and "廠名"='新竹' ;

select * from "22371-工業區污水處理廠放流水水質資訊" where "日期">20210101;

--8818-工業區污水處理廠分布位置圖

select "所在工業區" from "8818-工業區污水處理廠分布位置圖" where "所在工業區" like '%竹圍子%';
order by "所在工業區";

--95103-工業局所轄污水處理廠基本資料
select * from "95103-工業局所轄污水處理廠基本資料";
select "污水處理廠名稱" from "95103-工業局所轄污水處理廠基本資料" where "污水處理廠名稱" like '%大發%';

-- [DB]e_factory_base-EPAAPI:ems_s_01 環境保護許可管理系統(暨解除列管)對象基本資料 在  [DB]rivregln-中央管河川區域 裡面

select * from e_factory_base limit 1
select count(*) from e_factory_base
select rv_name from rivregln limit 10


CREATE OR REPLACE VIEW t_e_factory_base_in_river as
select S.*
from hackathon.e_factory_base as S
join rivregln as R
    on ST_Intersects(R.geom,S.geom)

OK SELECT UpdateGeometrySRID('rivregln','geom',3826);
select * from geometry_columns where f_table_name='e_factory_base';

-- 3826->4326
select 'SRID=3826;POINT(268552 2733033)'::geometry
select ST_AsEWKT(ST_Transform('SRID=3826;POINT(268552 2733033)'::geometry, 4326)) as wkt

-- DB output CSV
\copy ( select * from rivercode) To 'output/rivercode.csv' With CSV HEADER

-- 地理欄位都叫做 geom
select * from geometry_columns where f_table_name='county_moi';
select * from geometry_columns where not f_geometry_column='sim_geom'

with t1 as (
    SELECT ST_Npoints(geom) AS ori,
           ST_Npoints(sim_geom) AS sim,
           ST_NPoints(ST_Simplify(geom,0.0001,True)) AS ori_sim,countyname
    from county_moi
)
select avg(ori),avg(sim),avg(ori_sim) from t1;

SELECT UpdateGeometrySRID('village_moi_121','geom',3826);
SELECT UpdateGeometrySRID('town_moi','geom',3824);
SELECT UpdateGeometrySRID('county_moi','geom',3824);
UPDATE village_moi_121 SET sim_geom = ST_Simplify(geom,15,True);
UPDATE town_moi SET sim_geom = ST_Simplify(geom,0.0001,True);
UPDATE county_moi SET sim_geom = ST_Simplify(geom,0.0001,True);

town_moi,county_moi,village_moi_121
-- column rename
ALTER TABLE village_moi_121 RENAME COLUMN bbox TO sim_geom;

DROP TABLE IF EXISTS highway_test;
DROP TABLE [ IF EXISTS ] name [, ...] [ CASCADE | RESTRICT ]

--
SELECT table_schema,table_name,column_name,data_type FROM information_schema.columns where column_name like '%date%' and table_schema in ('public','hackathon')

-- 頭前溪水質測站位置
select sitename,twd97lon,twd97lat,twd97tm2x,twd97tm2y,ST_AsEWKT(ST_Transform(geom, 4326)) as wkt_4326,ST_AsEWKT(geom) as wkt_3826 from e_river_station where basin='頭前溪流域';
\copy ( select * from e_river_station where basin='頭前溪流域') To 'output/test.csv' With CSV HEADER

-- srid transfer
SELECT ST_AsEWKT('SRID=3826;POINT(268454 2733084)'::geometry);

