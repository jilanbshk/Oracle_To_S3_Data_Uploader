"""#############################################################################
#Oracle-to-S3 Data Uploader (v1.2, beta, 04/05/2016 15:11:53) [64bit] 
#Copyright (c): 2016 Alex Buzunov. Free to use, change or distribute.
#Agreement: Use this tool at your own risk. Author is not liable for any damages 
#           or losses related to the use of this software.
################################################################################
Usage:  
  set AWS_ACCESS_KEY_ID=<you access key>
  set AWS_SECRET_ACCESS_KEY=<you secret key>
  set ORACLE_LOGIN=tiger/scott@orcl
  set ORACLE_CLIENT_HOME=C:\\app\\oracle12\\product\\12.1.0\\dbhome_1
  
  oracle_to_s3_uploader.exe [<ora_query_file>] [<ora_col_delim>] [<ora_add_header>] 
			    [<s3_bucket_name>] [<s3_key_name>] [<s3_use_rr>] [<s3_public>]
	
	--ora_query_file -- SQL query to execure in source Oracle db.
	--ora_col_delim  -- CSV column delimiter (|).
	--ora_add_header -- Add header line to CSV file (False).
	--ora_lame_duck  -- Limit rows for trial upload (1000).
	--create_data_dump -- Use it if you want to persist streamed data on your filesystem.
	
	--s3_bucket_name -- S3 bucket name (always set it).
	--s3_location	 -- New bucket location name (us-west-2)
				Set it if you are creating new bucket
	--s3_key_name 	 -- CSV file name (to store query results on S3).
		if <s3_key_name> is not specified, the oracle query filename (ora_query_file) will be used.
	--s3_use_rr -- Use reduced redundancy storage (False).
	--s3_write_chunk_size -- Chunk size for multipart upoad to S3 (10<<21, ~20MB).
	--s3_public -- Make uploaded file public (False).
	
	Oracle data uploaded to S3 is always compressed (gzip).
"""

import sys

import os, sys
from pprint import pprint
from optparse import OptionParser
from pprint import pprint
import re	
import subprocess
from subprocess import PIPE,Popen

import gzip
import time
from datetime import datetime
	
class ImproperlyConfigured(Exception):
    """Base class for Boto exceptions in this module."""
    pass
	
try:
	import boto
	from boto.s3.key import Key
except ImportError:
	raise ImproperlyConfigured("Could not load Boto's S3 bindings")

try:
    import cStringIO
except ImportError:
    import io as cStringIO
	
e=sys.exit

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY') 
assert AWS_SECRET_ACCESS_KEY, 'AWS_SECRET_ACCESS_KEY is not set'
assert AWS_ACCESS_KEY_ID, 'AWS_ACCESS_KEY_ID is not set'
ORACLE_LOGIN = os.getenv('ORACLE_LOGIN') 
assert ORACLE_LOGIN, 'ORACLE_LOGIN is not set.'
ORACLE_CLIENT_HOME = os.getenv('ORACLE_CLIENT_HOME') 
assert ORACLE_CLIENT_HOME, 'ORACLE_CLIENT_HOME is not set'

bucket=None	
s3_key_name=None
use_rr=False,
make_public=True
total_size=0 #total uncompressed stream size
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
	p2 = Popen([ r'C:\app\oracle12\product\12.1.0\dbhome_1\BIN\sqlplus.exe', "-s", 'c##test/scott@orcl12'], stdin=p1.stdout, stdout=PIPE,stderr=PIPE)
	output=' '
	status=0
	cols=[]
	if 1:
		while output:
			output = p2.stdout.readline().strip() #.decode("utf-8")
			if output:
				cols.append(output.split(':')) 
	
	p1.wait()	
	p2.wait()
	return cols





import math

def convertSize(size):
	if (size == 0):
		return '0B'
	size_name = ("KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
	i = int(math.floor(math.log(size,1024)))
	p = math.pow(1024,i)
	s = round(size/p,2)
	return '%s %s' % (s,size_name[i])

def sendStreamGz(bucket, s3_key, pipe, suffix='.gz'):
	key = s3_key +suffix
	use_rr=False
	if opt.s3_use_rr:
		use_rr=True
	mpu = bucket.initiate_multipart_upload(key,reduced_redundancy=use_rr)
	stream = cStringIO.StringIO()
	compressor = gzip.GzipFile(fileobj=stream, mode='w')
	
	def uploadPart(partCount=[0]):
		global total_size
		partCount[0] += 1
		stream.seek(0)
		mpu.upload_part_from_file(stream, partCount[0])
		print('Uncompressed data size: %s' % convertSize(total_size))
		print('Compressed data size: %s' % convertSize(stream.tell()))
		stream.seek(0)
		stream.truncate()
	def upload_to_s3(dump_file=None):
		global total_size
		i=0
		
		while True:  # until EOF
			i+=1
			start_time = time.time()
			#chunk = inputFile.read(8192)
			chunk=pipe.read(opt.s3_write_chunk_size)
			#print(i)
			if not chunk:  # EOF?
				compressor.close()
				uploadPart()
				mpu.complete_upload()
				
				break
			compressor.write(chunk)
			total_size +=len(chunk)
			
			#print(compressor.tell())
			if dump_file:
				dump_file.write(chunk)
			#print(len(chunk))
			if stream.tell() > 10<<20:  # min size for multipart upload is 5242880
				
				uploadPart()
			print ('%d chunk %s [%s sec]' % (i, convertSize(len(chunk)),round((time.time() - start_time),2)))
	#with file(fileName) as inputFile:
	q_file =os.path.splitext(os.path.basename(opt.ora_query_file))[0]
	dump_dir=os.path.join('data_dump',q_file,opt.s3_bucket_name)
	if opt.create_data_dump:
		
		tss=datetime.now().strftime('%Y%m%d_%H%M%S')
		
		if not os.path.isdir(dump_dir):
			os.makedirs(dump_dir)
		dump_fn= os.path.join(dump_dir,'%s.%s.gz' % (  s3_key,tss) )
		#with open(dump_fn, 'w') as f:
		print ('Dumping data to: %s' % os.path.abspath(dump_fn) )
		with gzip.GzipFile(dump_fn, mode='w') as c:
			upload_to_s3(c)
	else:
		upload_to_s3(None)

	return key
	
def get_ora_pipe():

	in_qry=open(opt.ora_query_file, "r").read().strip().strip(';')

	cols=get_query_columns('', in_qry)

	col_str=('||\'%s\'||' % opt.ora_col_delim).join([c[0] for c in cols])
	header_str=''
	if opt.ora_add_header:
		header_str='PROMPT '+ ('%s' % opt.ora_col_delim).join([c[0] for c in cols])
	
	limit=''
	if opt.ora_lame_duck>0:
		limit='WHERE rownum<=%d' % opt.ora_lame_duck
	q="""
	set heading off line 32767 echo off feedback off  feed off pagesize 0 serveroutput off show off 
	set define off head off serveroutput off
	SET LONG 50000	
	SET VERIFY OFF
	%s
	SELECT %s str FROM (%s) %s;
	exit;
	""" % (header_str,col_str, in_qry,limit)

	p1 = Popen(['echo', q], stdout=PIPE,stderr=PIPE)

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

def RepresentsInt(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False
	
if __name__ == "__main__":		
	parser = OptionParser()
	parser.add_option("-q", "--ora_query_file", dest="ora_query_file", type=str)
	parser.add_option("-d", "--ora_col_delim", dest="ora_col_delim", type=str, default='|')
	parser.add_option("-e", "--ora_add_header", dest="ora_add_header",  action="store_true", default=False)
	parser.add_option("-l", "--ora_lame_duck", dest="ora_lame_duck", type=int, default=0)
	parser.add_option("-s", "--create_data_dump", dest="create_data_dump",  action="store_true", default=False)
	
	parser.add_option("-b", "--s3_bucket_name", dest="s3_bucket_name", type=str)
	parser.add_option("-t", "--s3_location", dest="s3_location", type=str, default='us-west-2')
	
	parser.add_option("-k", "--s3_key_name", dest="s3_key_name", type=str)
	parser.add_option("-w", "--s3_write_chunk_size", dest="s3_write_chunk_size", type=int, default=10<<20)
	
	parser.add_option("-r", "--s3_use_rr", dest="s3_use_rr",  action="store_true", default=False)
	parser.add_option("-p", "--s3_public", dest="s3_public",  action="store_true", default=False)
					  
	(opt, _) = parser.parse_args()
	#print (args)
	if len(sys.argv) < 2:
		print (__doc__)
		sys.exit()
	kwargs = dict(use_rr=opt.s3_use_rr, make_public=opt.s3_public)

	start_time = time.time()
	assert opt.ora_query_file, 'Input query file (-q,--ora_query_file) is not set.'
	assert os.path.isfile(opt.ora_query_file), 'Query file "%s"\ndoes not exists.' % opt.ora_query_file
	q_file=os.path.splitext(os.path.basename(opt.ora_query_file))
	

	assert opt.s3_bucket_name, 'Target S3 bucket name (-b,--s3_bucket_name) is not set.'
	assert RepresentsInt(opt.ora_lame_duck), '[-l] --ora_lame_duck is not of type "integer".'
	if not opt.s3_key_name:
		opt.s3_key_name =  q_file[0]
	from boto.s3.connection import Location
	
	conn = boto.connect_s3(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)
	#conn = boto.s3.connect_to_region('us-west-2') #'ap-northeast-1')
	
	from boto.exception import S3ResponseError, S3CreateError, BotoClientError
	#print(Location.USWest2)
	#pprint(dir(Location))
	#print(Location.__dict__.values())
	#print([x for x in Location.__dict__.values() if x and type(x)==str and len(x)>3 and len(x.split('.'))<2])
	#assert 'us-west-2' in Location.__dict__.values(), 'Location "%s" does not exists'
	try:
		bucket = conn.get_bucket(opt.s3_bucket_name)
		print ('Uploading results of "%s" to existing bucket "%s"' % (''.join(q_file),opt.s3_bucket_name))
	except S3ResponseError,err:
		
		if str(err).strip().endswith('404 Not Found'):
			print('Creating new bucket "%s" in location "%s"' % (opt.s3_bucket_name,opt.s3_location))
			try:
				conn.create_bucket(opt.s3_bucket_name, location=opt.s3_location)
				print ('Uploading results of "%s" to new bucket "%s" in region "%s"' % (''.join(q_file),opt.s3_bucket_name,opt.s3_location))
			except S3CreateError:
				print('Warning: Bucket "%s" already exists in "%s". Cannot proceed.' % (opt.s3_bucket_name,opt.s3_location))
				e(0)
		
			
	bucket = conn.get_bucket(opt.s3_bucket_name)
	p=get_ora_pipe()
	pipe=p.stdout
	s3key=sendStreamGz(bucket, opt.s3_key_name, pipe, suffix='.gz')
	#p1.wait()
	p.wait()
	#e(0)
	if opt.s3_public:
		k = Key(bucket)
		k.key = s3key
		k.make_public()

	sys.stdout.write('Upload complete (%s sec).\n' % round((time.time() - start_time),2))
	location='/'.join((opt.s3_bucket_name, '%s.gz' % opt.s3_key_name))
	file_type= 'PRIVATE'
	if opt.s3_public:
		file_type = 'PUBLIC'
	if 1: #options.make_public and location :
		_,region,aws,com =bucket.get_website_endpoint().split('.')		
		sys.stdout.write('Your %s upload is at: https://s3-%s.%s.%s/%s\n' % (file_type,opt.s3_location,aws,com,location))
