python oracle_to_redshift_loader.py ^
-q table_query.sql ^
-d "," ^
-b testbucket ^
-k oracle_table_export ^
-r ^
-o crime_test ^
-m "DD/MM/YYYY HH12:MI:SS" ^
-s