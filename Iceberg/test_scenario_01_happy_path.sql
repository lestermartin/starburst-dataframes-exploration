-- EASY SCENARIOS >> TPCH connector tables

-- Modify mycatalog.myschema as appropriate
DROP SCHEMA mycatalog.myschema CASCADE;
CREATE SCHEMA mycatalog.myschema;
USE mycatalog.myschema;



create table tpch_nation_hive_textfile
WITH (type='hive', format='textfile')
AS SELECT * FROM tpch.tiny.nation;

create table tpch_nation_hive_json
WITH (type='hive', format='json')
AS SELECT * FROM tpch.tiny.nation;

create table tpch_cust_hive_textfile
WITH (type='hive', format='textfile')
AS SELECT * FROM tpch.tiny.customer;

create table tpch_cust_hive_json
WITH (type='hive', format='json')
AS SELECT * FROM tpch.tiny.customer;

create table tpch_cust_hive_avro
WITH (type='hive', format='avro')
AS SELECT * FROM tpch.tiny.customer;

create table tpch_cust_hive_orc
WITH (type='hive', format='orc')
AS SELECT * FROM tpch.tiny.customer;

create table tpch_cust_hive_parquet
WITH (type='hive', format='parquet')
AS SELECT * FROM tpch.tiny.customer;

create table tpch_cust_ice_parquet
WITH (type='iceberg', format='parquet')
AS SELECT * FROM tpch.tiny.customer;

create table tpch_cust_ice_orc
WITH (type='iceberg', format='orc')
AS SELECT * FROM tpch.tiny.customer;

create table tpch_cust_ice_avro
WITH (type='iceberg', format='avro')
AS SELECT * FROM tpch.tiny.customer;

create table tpch_cust_delta
WITH (type='delta')
AS SELECT * FROM tpch.tiny.customer;

create table tpch_orders_delta
WITH (type='delta')
AS SELECT * FROM tpch.tiny.orders;

create table tpch_orders_ice_avro
WITH (type='iceberg', format='avro')
AS SELECT * FROM tpch.tiny.orders;

create table tpch_orders_ice_parquet
WITH (type='iceberg', format='parquet')
AS SELECT * FROM tpch.tiny.orders;

create table tpch_orders_ice_orc
WITH (type='iceberg', format='orc')
AS SELECT * FROM tpch.tiny.orders;

create table tpch_orders_hive_avro
WITH (type='hive', format='avro')
AS SELECT * FROM tpch.tiny.orders;

create table tpch_orders_hive_parquet
WITH (type='hive', format='parquet')
AS SELECT * FROM tpch.tiny.orders;

create table tpch_orders_hive_orc
WITH (type='hive', format='orc')
AS SELECT * FROM tpch.tiny.orders;

create table tpch_orders_hive_json
WITH (type='hive', format='json')
AS SELECT * FROM tpch.tiny.orders;

create table tpch_orders_hive_textfile
WITH (type='hive', format='textfile')
AS SELECT * FROM tpch.tiny.orders;
