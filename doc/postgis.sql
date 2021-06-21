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



