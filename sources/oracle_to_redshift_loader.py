"""#############################################################################
#Oracle-to-Redshift Data Loader (v1.2, beta, 04/05/2016 15:11:53) [64bit] 
#Copyright (c): 2016 Alex Buzunov, All rights reserved.
#Agreement: Use this tool at your own risk. Author is not liable for any damages 
#           or losses related to the use of this software.
################################################################################
Usage:  
  set AWS_ACCESS_KEY_ID=<you access key>
  set AWS_SECRET_ACCESS_KEY=<you secret key>
  set ORACLE_LOGIN=tiger/scott@orcl
  set ORACLE_CLIENT_HOME=C:\\app\\oracle12\\product\\12.1.0\\dbhome_1
  set NLS_DATE_FORMAT="MM/DD/YYYY HH12:MI:SS"
  set NLS_TIMESTAMP_FORMAT="MM/DD/YYYY HH12:MI:SS"
  set NLS_TIMESTAMP_TZ_FORMAT="MM/DD/YYYY HH12:MI:SS.FF TZH:TZM"
  
  set REDSHIFT_CONNECT_STRING="dbname='***' port='5439' user='***' password='***' host='mycluster.***.redshift.amazonaws.com'"  
  
  
  oracle_to_redshift_loader.exe [<ora_query_file>] [<ora_col_delim>] [<ora_add_header>] 
			    [<s3_bucket_name>] [<s3_key_name>] [<s3_use_rr>] [<s3_public>]
	
	--ora_query_file -- SQL query to execure in source Oracle db.
	--ora_col_delim  -- CSV column delimiter for downstream(,).
	--ora_quote	-- Enclose values in quotes (")
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
	
	--red_to_table  -- Target Amazon-Redshit table name.
	--red_col_delim  -- CSV column delimiter for upstream(,).
	--red_quote 	-- Set it if input values are quoted.
	--red_timeformat -- Timestamp format for Redshift ('MM/DD/YYYY HH12:MI:SS').
	--red_ignoreheader -- skip header in input stream
	
	Oracle data uploaded to S3 is always compressed (gzip).

	Boto S3 docs: http://boto.cloudhackers.com/en/latest/ref/s3.html
	psycopg2 docs: http://initd.org/psycopg/docs/

"""

	
#>type c:\tmp\data.csv| python file_upload.py
import sys
#data = sys.stdin.readlines()
#print "Counted", len(data), "lines."

import os, sys
from pprint import pprint
from optparse import OptionParser
from pprint import pprint
import re	
import subprocess
from subprocess import PIPE,Popen
import psycopg2
import select
import gzip
import time
from datetime import datetime
import math
import __builtin__
	
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
import imp

REDSHIFT_CONNECT_STRING= os.getenv('REDSHIFT_CONNECT_STRING') 
assert REDSHIFT_CONNECT_STRING, 'REDSHIFT_CONNECT_STRING is not set'

bucket=None	
#bucket_name= 'pythonuploadtest1' 
s3_key_name=None
use_rr=False,
make_public=True
total_size=0 #total uncompressed stream size
#env={}
for k,v in os.environ.items():
	#print k,v
	if k.upper().startswith('NLS'):
		os.environ[k]=v.strip().strip('"').strip("'")
		#print env[k]
	#else:
	#	env[k]=v
#e(0)
def import_module(filepath):
	class_inst = None
	#expected_class = 'MyClass'

	mod_name,file_ext = os.path.splitext(os.path.split(filepath)[-1])
	assert os.path.isfile(filepath), 'File %s does not exists.' % filepath
	if file_ext.lower() == '.py':
		py_mod = imp.load_source(mod_name, filepath)

	elif file_ext.lower() == '.pyc':
		py_mod = imp.load_compiled(mod_name, filepath)
	return py_mod
	
def wait(conn):
	while 1:
		state = conn.poll()
		if state == psycopg2.extensions.POLL_OK:
			break
		elif state == psycopg2.extensions.POLL_WRITE:
			select.select([], [conn.fileno()], [])
		elif state == psycopg2.extensions.POLL_READ:
			select.select([conn.fileno()], [], [])
		else:
			raise psycopg2.OperationalError("poll() returned %s" % state)
		#update_progress_bar()



def convertSize(size):
	if (size == 0):
		return '0B'
	size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
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
		print('Size: Uncompressed: %s' % convertSize(total_size))
		print('Size: Compressed  : %s' % convertSize(stream.tell()))
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
			#print(len(chunk),opt.s3_write_chunk_size)
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
	#print('Compressed size: %s' %compressor.fileobj.tell())
	#print(dir(compressor))
	
	return key
	


def RepresentsInt(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False 

	
	
if __name__ == "__main__":		
	parser = OptionParser()
	parser.add_option("-q", "--ora_query_file", dest="ora_query_file", type=str)
	parser.add_option("-d", "--ora_col_delim", dest="ora_col_delim", type=str, default=',')
	parser.add_option("-a", "--ora_quote", dest="ora_quote", default='"')	

	parser.add_option("-e", "--ora_add_header", dest="ora_add_header",  action="store_true", default=False)
	parser.add_option("-l", "--ora_lame_duck", dest="ora_lame_duck", type=int, default=0)
	
	parser.add_option("-s", "--create_data_dump", dest="create_data_dump",  action="store_true", default=False)
	
	parser.add_option("-b", "--s3_bucket_name", dest="s3_bucket_name", type=str)
	parser.add_option("-t", "--s3_location", dest="s3_location", type=str, default='us-west-2')
	
	parser.add_option("-k", "--s3_key_name", dest="s3_key_name", type=str)
	parser.add_option("-w", "--s3_write_chunk_size", dest="s3_write_chunk_size", type=int, default=10<<20)
	
	parser.add_option("-r", "--s3_use_rr", dest="s3_use_rr",  action="store_true", default=False)
	parser.add_option("-p", "--s3_public", dest="s3_public",  action="store_true", default=False)
	parser.add_option("-o", "--red_to_table", dest="red_to_table")
	parser.add_option("-c", "--red_col_delim", dest="red_col_delim", type=str, default=',')
	parser.add_option("-u", "--red_quote", dest="red_quote",type=str, default='"')
	parser.add_option("-m", "--red_timeformat", dest="red_timeformat", default='MM/DD/YYYY HH12:MI:SS')
	parser.add_option("-i", "--red_ignoreheader", dest="red_ignoreheader", type=int, default=0)	
					  
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
	
	assert opt.red_to_table, 'Target Redshift table '
	assert opt.s3_bucket_name, 'Target S3 bucket name (-b,--s3_bucket_name) is not set.'
	assert RepresentsInt(opt.ora_lame_duck), '[-l] --ora_lame_duck is not of type "integer".'
	if not opt.s3_key_name:
		opt.s3_key_name =  q_file[0]
	from boto.s3.connection import Location
	
	conn = boto.connect_s3(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)
	#conn = boto.s3.connect_to_region('us-west-2') #'ap-northeast-1')
	
	from boto.exception import S3ResponseError, S3CreateError, BotoClientError

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
	p=None
	if 	1:
		__builtin__.g = globals()
		abspath=os.path.abspath(os.path.dirname(sys.argv[0]))
		extractor_file = os.path.join(abspath,'include','extractor.py')		
		extractor=import_module(extractor_file)
		p=extractor.extract(os.environ)

		
	
	sys.stdout.write('Started reading from Oracle (%s sec).\n' % round((time.time() - start_time),2))

	pipe=p.stdout
	s3key=sendStreamGz(bucket, opt.s3_key_name, pipe, suffix='.gz')
	#p1.wait()
	p.wait()
	#sys.stdout.write('Done reading from Oracle (%s sec).\n' % round((time.time() - start_time),2))
	#e(0)
	if opt.s3_public:
		k = Key(bucket)
		k.key = s3key
		k.make_public()

	sys.stdout.write('Elapsed: Oracle+S3    :%s sec.\n' % round((time.time() - start_time),2))
	location='/'.join((opt.s3_bucket_name, '%s.gz' % opt.s3_key_name))
	file_type= 'PRIVATE'
	if opt.s3_public:
		file_type = 'PUBLIC'
		if 1: #options.make_public and location :
			_,region,aws,com =bucket.get_website_endpoint().split('.')		
			sys.stdout.write('Your %s upload is at: https://s3-%s.%s.%s/%s\n' % (file_type,opt.s3_location,aws,com,location))
	if location:
		import __builtin__
		__builtin__.g = globals()
		abspath=os.path.abspath(os.path.dirname(sys.argv[0]))
		loader_file = os.path.join(abspath,'include','loader.py')		
		loader=import_module(loader_file)
		loader.load(location)
		print ('Elapsed: S3->Redshift :%s sec.' % round((time.time() - start_time),2))
	sys.stdout.write('--------------------------------\nTotal elapsed: %s sec.\n' % round((time.time() - start_time),2))