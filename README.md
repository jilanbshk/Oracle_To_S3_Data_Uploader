# Oracle-to-S3 table data uploader.
Let's you stream your Oracle table data to Amazon-S3 from Windows command line.


Features:
 - Streams Oracle table data to Amazon-S3.
 - No need to create CSV extracts before upload to S3.
 - Data stream is compressed while upload to S3.
 - No need for Amazon AWS CLI.
 - Works from your OS Windows desktop (command line).
 - It's executable (Oracle_To_S3_Uploader.exe)  - no need for Python install.
 - It's 64 bit - it will work on any vanilla DOS for 64-bit Windows.
 - AWS Access Keys are not passed as arguments. 
 - Written using Python/boto/PyInstaller.


##Version

OS|Platform|Version 
---|---|---- | -------------
Windows|32bit|[0.1.0 beta]

##Purpose

- Stream (upload) Oracle table data to Amazon-S3.

## How it works
- Tool connects to source Oracle DB and opens data pipe for reading.
- Data is pumped to S3 using multipart upload.
- Optional upload to Reduced Redundancy storage (not RR by default).
- Optional "make it public" after upload (private by default)
- If doesn't, bucket is created
- You can control the region where new bucket is created
- Streamed data can be tee'd (dumped on disk) during upload.
- If not set, S3 Key defaulted to query file name.
- It's a Python/boto script
	* Boto S3 docs: http://boto.cloudhackers.com/en/latest/ref/s3.html
- Executable is created using [pyInstaller] (http://www.pyinstaller.org/)

##Audience

Database/ETL developers, Data Integrators, Data Engineers, Business Analysts, AWS Developers, DevOps, 

##Designated Environment
Pre-Prod (UAT/QA/DEV)

##Usage

```
c:\Python35-32\PROJECTS\Ora2S3>dist\oracle_to_s3_uploader.exe
#############################################################################
#Oracle to S3 Data Uploader (v1.2, beta, 04/05/2016 15:11:53) [64bit]
#Copyright (c): 2016 Alex Buzunov, All rights reserved.
#Agreement: Use this tool at your own risk. Author is not liable for any damages
#           or losses related to the use of this software.
################################################################################
Usage:
  set AWS_ACCESS_KEY_ID=<you access key>
  set AWS_SECRET_ACCESS_KEY=<you secret key>
  set ORACLE_LOGIN=tiger/scott@orcl
  set ORACLE_CLIENT_HOME=C:\app\oracle12\product\12.1.0\dbhome_1

  oracle_to_s3_uploader.exe [<ora_query_file>] [<ora_col_delim>] [<ora_add_header>]
                            [<s3_bucket_name>] [<s3_key_name>] [<s3_use_rr>] [<s3_public>]

        --ora_query_file -- SQL query to execure in source Oracle db.
        --ora_col_delim  -- CSV column delimiter (|).
        --ora_add_header -- Add header line to CSV file (False).
        --ora_lame_duck  -- Limit rows for trial upload (1000).
        --create_data_dump -- Use it if you want to persist streamed data on your filesystem.

        --s3_bucket_name -- S3 bucket name (always set it).
        --s3_location    -- New bucket location name (us-west-2)
                                Set it if you are creating new bucket
        --s3_key_name    -- CSV file name (to store query results on S3).
                if <s3_key_name> is not specified, the oracle query filename (ora_query_file) will be used.
        --s3_use_rr -- Use reduced redundancy storage (False).
        --s3_write_chunk_size -- Chunk size for multipart upoad to S3 (10<<21, ~20MB).
        --s3_public -- Make uploaded file public (False).

        Oracle data uploaded to S3 is always compressed (gzip).

```
#Example


###Environment variables

* Set the following environment variables (for all tests:

```
set AWS_ACCESS_KEY_ID=<you access key>
set AWS_SECRET_ACCESS_KEY=<you secret key>

set ORACLE_LOGIN=tiger/scott@orcl
set ORACLE_CLIENT_HOME=C:\\app\\oracle12\\product\\12.1.0\\dbhome_1
```

### Test upload with data dump.



```
set AWS_ACCESS_KEY_ID=<you access key>
set AWS_SECRET_ACCESS_KEY=<you secret key>

set ORACLE_LOGIN=tiger/scott@orcl
set ORACLE_CLIENT_HOME=C:\\app\\oracle12\\product\\12.1.0\\dbhome_1

c:\Python35-32\PROJECTS\Ora2S3>dist\oracle_to_s3_uploader.exe -q table_query.sql -d "|" -e -b test_bucket -k oracle_table_export -r -p  -s
Uploading results of "table_query.sql" to bucket: test_bucket
Dumping data to: c:\Python35-32\PROJECTS\Ora2S3\data_dump\table_query\test_bucket\oracle_table_export.20160405_233607.gz
1 chunk 10.0 GB [9.0 sec]
2 chunk 5.94 GB [5.37 sec]
Uncompressed data size: 15.94 GB
Compressed data size: 63.39 MB
Upload complete (17.45 sec).
Your PUBLIC upload is at: https://s3-us-west-2.amazonaws.com/test_bucket/oracle_table_export.gz
```

####Test query

Contents of the file *table_query.sql*:

```
SELECT * FROM test2 WHERE rownum<100000;

```

###Download
* `git clone https://github.com/alexbuz/CSV_Loader_For_Redshift`
* [Master Release](https://github.com/alexbuz/Oracle_To_S3_Data_Uploader/archive/master.zip) -- `csv_loader_for_redshift 0.1.0`




#
#
#
#
#   
#FAQ
#  
#### Can it load CSV file from Windows desktop to Amazon Redshift.
Yes, it is the main purpose of this tool.

#### Can developers integrate CSV loader into their ETL pipelines?
Yes. Assuming they are doing it on OS Windows.

#### How fast is data upload using `CSV Loader for Redshift`?
As fast as any AWS API provided by Amazon.

####How to inscease upload speed?
Compress input file or provide `-z` or `--gzip_source_file` arg in command line and this tool will compress it for you before upload to S3.

#### What are the other ways to upload file to Redshift?
You can use 'aws s3api' and psql COPY command to do pretty much the same.

#### Can I just zip it using Windows File Explorer?
No, Redshift will not recognize *.zip file format.
You have to `gzip` it. You can use 7-Zip to do that.


#### Does it delete file from S3 after upload?
No

#### Does it create target Redshift table?
No

#### Is there an option to compress input CSV file before upload?
Yes. Use `-z` or `--gzip_source_file` argument so the tool does compression for you.


#### Explain first step of data load?
The CSV you provided is getting preloaded to Amazon-S3.
It doesn't have to be made public for load to Redshift. 
It can be compressed or uncompressed.
Your input file is getting compressed (optional) and uploaded to S3 using credentials you set in shell.


#### Explain second step of data load. How data is loaded to Amazon Redshift?
You Redshift cluster has to be open to the world (accessible via port 5439 from internet).
It uses PostgreSQL COPY command to load file located on S3 into Redshift table.


#### Can I use WinZip or 7-zip
Yes, but you have to use 'gzip' compression type.

#### What technology was used to create this tool
I used Python, Boto, and psycopg2 to write it.
Boto is used to upload file to S3. 
psycopg2 is used to establish ODBC connection with Redshift clusted and execute `COPY` command.

#### Where are the sources?
Please, contact me for sources.

#### Can you modify functionality and add features?
Yes, please, ask me for new features.

#### What other AWS tools you've created?
- [S3_Sanity_Check] (https://github.com/alexbuz/S3_Sanity_Check/blob/master/README.md) - let's you `ping` Amazon-S3 bucket to see if it's publicly readable.
- [EC2_Metrics_Plotter](https://github.com/alexbuz/EC2_Metrics_Plotter/blob/master/README.md) - plots any CloudWatch EC2 instance  metric stats.
- [S3_File_Uploader](https://github.com/alexbuz/S3_File_Uploader/blob/master/README.md) - uploads file from Windows to S3.

#### Do you have any AWS Certifications?
Yes, [AWS Certified Developer (Associate)](https://raw.githubusercontent.com/alexbuz/FAQs/master/images/AWS_Ceritied_Developer_Associate.png)

#### Can you create similar/custom data tool for our business?
Yes, you can PM me here or email at `alex_buz@yahoo.com`.
I'll get back to you within hours.

###Links
 - [Employment FAQ](https://github.com/alexbuz/FAQs/blob/master/README.md)

















