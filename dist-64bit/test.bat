
dist\oracle_to_s3_uploader.exe ^
	-q table_query.sql ^
	-d "|" ^
	-e ^
	-b target_test_bucket ^
	-k oracle_table_export ^
	-r ^
	-p ^
	-s
