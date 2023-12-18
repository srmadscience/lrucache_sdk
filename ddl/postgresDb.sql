CREATE TABLE subscriber  ( 
  s_id    BIGINT NOT NULL PRIMARY KEY
, sub_nbr VARCHAR(80) NOT NULL
, f_tinyint smallint
, f_smallint smallint
, f_integer integer
, f_bigint bigint
, f_float float
, f_decimal decimal
, f_geography text
, f_geography_point text
, f_varchar varchar(80)
, f_varbinary bytea
, last_use_date TIMESTAMP NOT NULL) 