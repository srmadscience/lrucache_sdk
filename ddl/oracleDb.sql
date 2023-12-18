create user vtest identified by vtest;

create tablespace vtest_data  
datafile '/home/oracle/oradata/orcl/vtest_data_01.dbf' size 2G autoextend on online;


create tablespace perfstat  
datafile '/u01/app/oracle/oradata/orcl/perfstat.dbf' size 1G autoextend on online;


alter user vtest default tablespace vtest_data;

alter user vtest   quota unlimited on vtest_data;


grant create session to vtest;

CREATE TABLE vtest.subscriber  ( 
  s_id    NUMBER NOT NULL PRIMARY KEY
, sub_nbr VARCHAR2(80) NOT NULL
, f_tinyint NUMBER
, f_smallint NUMBER
, f_integer NUMBER
, f_bigint NUMBER
, f_float NUMBER
, f_decimal NUMBER
, f_geography VARCHAR2(80)
, f_geography_point VARCHAR2(80)
, f_varchar varchar2(80)
, f_varbinary raw(1024)
, flush_date TIMESTAMP(6)
, last_use_date TIMESTAMP(6) NOT NULL)
TABLESPACE vtest_data;

