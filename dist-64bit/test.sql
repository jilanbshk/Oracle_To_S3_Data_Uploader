CREATE TABLE test2 (id integer , num integer, data varchar2(4000),num2 integer, data2 varchar2(4000),num3 integer, data3 varchar2(4000),num4 integer, data4 varchar2(4000));

insert into test2 select * from test2;

select count(*) from test2;
