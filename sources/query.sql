--set  tab off head off line 4000 pages 0   feed off newpage 0 arraysize 5000
set heading off line 7000 echo off feedback off  feed off newpage 0 pagesize 99 serveroutput off show off 
--set colsep | 
set define off head off serveroutput off 
SET VERIFY OFF
SELECT id,num,data,num2,data2,num3,data3,num4,data4 FROM test2 WHERE rownum<10;
exit;