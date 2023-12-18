CREATE KEYSPACE vtest
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
  
  CREATE TABLE vtest.subscriber  (
  s_id    BIGINT
, sub_nbr text
, f_tinyint tinyint
, f_smallint smallint
, f_integer int
, f_bigint bigint
, f_float float
, f_decimal decimal
, f_geography text
, f_geography_point text
, f_varchar varchar
, f_varbinary blob
, flush_date timestamp
, last_use_date TIMESTAMP
, primary key (s_id)) ;
