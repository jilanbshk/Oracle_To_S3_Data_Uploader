# Oracle-to-S3 data uploader.
    Ground to cloud data integration tool.
    Let's you stream your Oracle table/query data to Amazon-S3 from Windows CLI (command line).


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
Windows|64bit|[1.2 beta]

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

* Set the following environment variables (for all tests):
set_env.bat:
```
set AWS_ACCESS_KEY_ID=<you access key>
set AWS_SECRET_ACCESS_KEY=<you secret key>

set ORACLE_LOGIN=tiger/scott@orcl
set ORACLE_CLIENT_HOME=C:\\app\\oracle12\\product\\12.1.0\\dbhome_1
```

### Test upload with data dump.
In this example complete table `test2` get's uploaded to Aamzon-S3 as compressed CSV file.

Contents of the file *table_query.sql*:

```
SELECT * FROM test2;

```
Also temporary dump file is created for analysis (by default there are no files created)
Use `-s, --create_data_dump` to dump streamed data.

If target bucket does not exists it will be created in user controlled region.
Use argument `-t, --s3_location` to set target region name

Contents of the file *test.bat*:
```
dist\oracle_to_s3_uploader.exe ^
	-q table_query.sql ^
	-d "|" ^
	-e ^
	-b test_bucket ^
	-k oracle_table_export ^
	-r ^
	-p ^
	-s
```
Executing `test.bat`:

```
c:\Python35-32\PROJECTS\Ora2S3>dist\oracle_to_s3_uploader.exe   -q table_query.sql      -d "|"  -e      -b test_bucket       -k oracle_table_export  -r      -p      -s
Uploading results of "table_query.sql" to existing bucket "test_bucket"
Dumping data to: c:\Python35-32\PROJECTS\Ora2S3\data_dump\table_query\test_bucket\oracle_table_export.20160405_235310.gz
1 chunk 10.0 GB [8.95 sec]
2 chunk 5.94 GB [5.37 sec]
Uncompressed data size: 15.94 GB
Compressed data size: 63.39 MB
Upload complete (17.58 sec).
Your PUBLIC upload is at: https://s3-us-west-2.amazonaws.com/test_bucket/oracle_table_export.gz
```



###Download
* `git clone https://github.com/alexbuz/Oracle_To_S3_Data_Uploader`
* [Master Release](https://github.com/alexbuz/Oracle_To_S3_Data_Uploader/archive/master.zip) -- `oracle_to_s3_uploader 1.2`




#
#
#
#
#   
#FAQ
#  
#### Can it load Oracle data to Amazon S3 file?
Yes, it is the main purpose of this tool.

#### Can developers integrate `Oracle_To_S3_Data_Uploader` into their ETL pipelines?
Yes. Assuming they are doing it on OS Windows.

#### How fast is data upload using `CSV Loader for Redshift`?
As fast as any implementation of multi-part load using Python and boto.

####How to inscease upload speed?
Input data stream is getting compressed before upload to S3. So not much could be done here.
You may want to run it closer to source or target for better performance.

#### What are the other ways to move large amounts of data from Oracle to S3?
You can write a sqoop script that can be scheduled as an 'EMR Activity' under Data Pipeline.

#### Does it create temporary data file to facilitate data load to S3?
No

#### Can I extract data from RDS Oracle to Amazon S3 using this tool?
Yes, but whatch where you invoke it. If you execute it outside of AWS you may get data charges.
You should spawn OS Windows EC2 instance in the same Availability Zone with your DRS Oracle.
Login to new EC2 instance usig Remote Desktop, download `Oracle_To_S3_Data_Uploader` and run it in CLI window.

#### Can I log transfered data for analysis?
Yes, Use `-s, --create_data_dump` to dump streamed data.

#### Explain first step of data transfer?
The query file you provided is used to select data form target Oracle server.
Stream is compressed before load to S3.

#### Explain second step of data transfer?
Compressed data is getting uploaded to S3 using multipart upload protocol.

#### What technology was used to create this tool
I used SQL*Plus, Python, Boto to write it.
Boto is used to upload file to S3. 
SQL*Plus is used to spool data to compressor pipe.

#### Where are the sources?
Please, contact me for sources.

#### Can you modify functionality and add features?
Yes, please, ask me for new features.

#### What other AWS tools you've created?
- [CSV_Loader_For_Redshift] (https://github.com/alexbuz/CSV_Loader_For_Redshift/blob/master/README.md) - Append CSV data to Amazon-Redshift from Windows.
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

















