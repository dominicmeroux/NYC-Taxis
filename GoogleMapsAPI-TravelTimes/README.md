**Prerequisites**: Have Python (2.x) installed, MySQL Workbench, and ensure you can correctly connect to MySQL using the MySQL Python connector. Obtain a Google Maps API key. For Step 1, you will need to use a Spark cluster to carry out this operation using parallel processing (or a really powerful computer with a lot of storage space and Python Spark running).  

1) Use the code in any of the Sample-2015NYCYellowCab.* files to obtain a simple random sample of the 2015 NYC Yellow Cab trip data. 

2) Use TaxiQueries.py up to line 49 to create a MySQL database. 

3) Uncomment appropriate segments of code in Database_Tables.sql to create tables in MySQL (an easy way would be to run this code in MySQL Workbench). 

4) You are now ready to run the full TaxiQueries.py code. 

5) After a successful run of TaxiQueries.py, run "BackupDatabase.py" to generate CSV files that represent your latest MySQL tables as a backup of your MySQL database. Due to the 2500 queries / day limit of the Google Maps API, you may run TaxiQueries.py for quite a few days in order to obtain a meaningful sample size, so backing up the Google Maps API query results is critical. 
