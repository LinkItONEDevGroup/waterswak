-- 環境與登入
export PATH=$PATH:/Applications/Postgres.app/Contents/Versions/13/bin
psql -h localhost -p 5431 -U postgres nyc

-- 官方範例
https://postgis.net/workshops/postgis-intro/workshop-sql.txt

----------------- 課程範例 ---------------
--
\d

-- save to CSV
\copy ( SELECT boroname, avg(char_length(name)) FROM nyc_neighborhoods GROUP BY boroname ) To '/tmp/test.csv' With CSV HEADER

-- map view
SELECT name,
       ST_Transform(geom, 4326)
FROM nyc_neighborhoods;
