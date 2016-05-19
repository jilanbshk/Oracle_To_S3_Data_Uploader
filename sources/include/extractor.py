import re, os
from subprocess import PIPE,Popen
# dict g - is a calling env Globals()
opt= g['opt']
ORACLE_CLIENT_HOME = g['ORACLE_CLIENT_HOME']
ORACLE_LOGIN = g['ORACLE_LOGIN']

def extract(env):
	
	in_qry=open(opt.ora_query_file, "r").read().strip().strip(';')
	
	cols=get_query_columns('', in_qry)
	
	col_str=('||\'%s%s%s\'||' % (opt.ora_quote,opt.ora_col_delim,opt.ora_quote)).join([c[0] for c in cols])
	header_str=''
	if opt.ora_add_header:
		header_str='PROMPT %s%s%s' % (opt.ora_quote,'%s%s%s' % (opt.ora_quote,opt.ora_col_delim,opt.ora_quote).join([c[0] for c in cols]),opt.ora_quote)
	#print (col_str)
	#e(0)	
	limit=''
	if opt.ora_lame_duck>0:
		limit='WHERE rownum<=%d' % opt.ora_lame_duck
	
	q="""
	set heading off line 32767 echo off feedback off  feed off pagesize 0 serveroutput off show off 
	set define off head off serveroutput off arraysize 5000
	SET LONG 50000	
	SET VERIFY OFF
	%s
	SELECT '%s'||%s||'%s' str FROM (%s) %s;
	exit;
	""" % (header_str,opt.ora_quote,col_str,opt.ora_quote, in_qry,limit)

	p1 = Popen(['echo', q], stdout=PIPE,stderr=PIPE,env=env)
	plus_loc=os.path.join(ORACLE_CLIENT_HOME,'bin','sqlplus.exe')
	assert os.path.isfile(plus_loc), 'Cannot locate sqlplus.exe at\n%s' % ORACLE_CLIENT_HOME
	p2 = Popen([ plus_loc, "-s", ORACLE_LOGIN], stdin=p1.stdout, stdout=PIPE,stderr=PIPE)
	output=' '
	status=0
	if 0:
		while output:
			output = p2.stdout.readline()
			#print output
			#e(0)
	p1.wait()
	return p2
def get_query_columns( login, qry):
	
	

	q="""DECLARE
  c           NUMBER;
  d           NUMBER;
  col_cnt     INTEGER;
  f           BOOLEAN;
  rec_tab     DBMS_SQL.DESC_TAB;
  col_num    NUMBER;
  v_sql dbms_sql.varchar2a;
 v_sql_1 varchar2(32767);
 v_sql_2 varchar2(32767);
 v_sql_3 varchar2(32767);
 v_sql_4 varchar2(32767);
  v_type VARCHAR2(32):='';
  PROCEDURE print_rec(rec in DBMS_SQL.DESC_REC) IS
  BEGIN
	v_type:=CASE rec.col_type
				WHEN 1 THEN 'VARCHAR2'
				WHEN 12 THEN 'DATE'
				WHEN 2 THEN 'NUMBER'
			ELSE ''||rec.col_type
			END;
    DBMS_OUTPUT.PUT_LINE(rec.col_name||':'||rec.col_max_len||':'||v_type);
  END;
BEGIN
  v_sql(1):='%s';
  v_sql(2):='%s';
  v_sql(3):='%s';
  v_sql(4):='%s';
  v_sql(5):='%s';
  c := DBMS_SQL.OPEN_CURSOR;
  DBMS_SQL.PARSE(c, v_sql,1,5,False, DBMS_SQL.NATIVE);
  d := DBMS_SQL.EXECUTE(c);
  DBMS_SQL.DESCRIBE_COLUMNS(c, col_cnt, rec_tab);
/*
 * Following loop could simply be for j in 1..col_cnt loop.
 * Here we are simply illustrating some of the PL/SQL table
 * features.
 */
  col_num := rec_tab.first;
  IF (col_num IS NOT NULL) THEN
    LOOP
      print_rec(rec_tab(col_num));
      col_num := rec_tab.next(col_num);
      EXIT WHEN (col_num IS NULL);
    END LOOP;
  END IF;
  DBMS_SQL.CLOSE_CURSOR(c);
END;
/

""" % (qry[0:32000].replace("'","''"),qry[32000:64000].replace("'","''"),qry[64000:96000].replace("'","''"),qry[96000:128000].replace("'","''"),qry[128000:160000].replace("'","''"))
	regexp=re.compile(r'([\w\_\:\(\)\d]+)')

	p1 = Popen(['echo', 'set serveroutput on echo on termout on feedback off\n%s' % q], stdout=PIPE,stderr=PIPE)
	plus_loc=os.path.join(ORACLE_CLIENT_HOME,'bin','sqlplus.exe')
	p2 = Popen([plus_loc, "-s", ORACLE_LOGIN], stdin=p1.stdout, stdout=PIPE,stderr=PIPE)
	output=' '
	status=0
	cols=[]
	if 1:
		while output:
			output = p2.stdout.readline().strip() #.decode("utf-8")
			if output:
				#print(output.decode("utf-8").split(':'))
				cols.append(output.split(':')) 
	
	p1.wait()	
	p2.wait()
	return cols

	