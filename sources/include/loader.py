import time, psycopg2
# dict g - is a calling env Globals()
opt= g['opt']
REDSHIFT_CONNECT_STRING = g['REDSHIFT_CONNECT_STRING']
AWS_ACCESS_KEY_ID = g['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = g['AWS_SECRET_ACCESS_KEY']

def load(location):
	start_time = time.time()
	fn='s3://%s' % location
	conn_string = REDSHIFT_CONNECT_STRING.strip().strip('"')	
	con = psycopg2.connect(conn_string);
	cur = con.cursor();	
	quote=''
	if opt.red_quote:
		quote='quote \'%s\'' % opt.red_quote
	ignoreheader =''
	if opt.red_ignoreheader:
		ignoreheader='IGNOREHEADER %s' % opt.red_ignoreheader
	timeformat=''
	if opt.red_timeformat:
		#timeformat=" dateformat 'auto' "
		timeformat=" timeformat '%s'" % opt.red_timeformat.strip().strip("'")
	sql="""
COPY %s FROM '%s' 
	CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' 
	DELIMITER '%s' 
	FORMAT CSV %s 
	GZIP 
	%s 
	%s; 
	COMMIT;
""" % (opt.red_to_table, fn, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,opt.red_col_delim,quote, timeformat, ignoreheader)
	#print (sql)
	cur.execute(sql)	
	con.close()	