# Sampling NYC Taxi data (COMING SOON)

# NYC Taxi Google Maps API Queries

**Goal**: Using a sample of NYC Taxi trips, query the Google Maps API to obtain travel times using 'driving', 'transit', 'cycling', and 'walking' modes. 

**Prerequisites**: Have Python (2.x) installed, MySQL Workbench, and ensure you can correctly connect to MySQL using the MySQL Python connector. Obtain a Google Maps API key. 

1) Use TaxiQueries.py up to line 49 to create a MySQL database. 

2) Uncomment appropriate segments of code in Database_Tables.sql to create tables in MySQL (an easy way would be to run this code in MySQL Workbench). 

3) You are now ready to run the full TaxiQueries.py code. 

4) After a successful run of TaxiQueries.py, run "BackupDatabase.py" to generate CSV files that represent your latest MySQL tables as a backup of your MySQL database. Due to the 2500 queries / day limit of the Google Maps API, you may run TaxiQueries.py for quite a few days in order to obtain a meaningful sample size, so backing up the Google Maps API query results is critical. 

# NYC Taxi Powertrain Analysis

**Goal**: This Python program uses PySpark to assess the proportion of New York City taxicabs that have a hybrid powertrain compared with the proportion that have other powertrains. 

**Prerequisites**: Download data from https://data.cityofnewyork.us/Transportation/Medallion-Vehicles-Authorized/rhe8-mgbb

Must install pyspark. In this case, PySpark version 2.0.0 was used.  
