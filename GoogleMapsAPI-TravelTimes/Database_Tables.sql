#use EquityDB;

################################# General Functionality
#show tables;
#select COUNT(*) from Trips; # 14,218 is the final total
#select * from NYC_GreenTaxi2015;   # 03/31/2015 03:12:38 PM
#select * from NYC_YellowTaxi2015;  # 2015-01-08 23:15:29
#select count(*) from NYC_GreenTaxi2015;  #  1,683
#select count(*) from NYC_YellowTaxi2015; # 12,535
#select STR_TO_DATE(pickup_datetime, '%Y-%m-%d %H:%i:%s') from NYC_YellowTaxi2015;
# Taxi table strategy: Use a query to create a seperate table that pulls together the values from all 3 taxi tables

################################# DROP TABLES
#drop table Trips; 
#drop table TripsBackup;
#drop table NYC_GreenTaxi2015;
#drop table NYC_YellowTaxi2015;
#drop table NYC_GreenTaxi2015Backup;
################################# CREATE TABLES

/*
CREATE TABLE IF NOT EXISTS Trips
(
Index_ID INTEGER,
ORIGIN_Latitude DOUBLE,
ORIGIN_Longitude DOUBLE,
DESTINATION_Latitude DOUBLE,
DESTINATION_Longitude DOUBLE,
Driving_Distance FLOAT,
Driving_Duration FLOAT,
Driving_Duration_in_Traffic FLOAT,
Transit_Distance FLOAT,
Transit_Duration FLOAT,
Cycling_Distance FLOAT,
Cycling_Duration FLOAT,
Walking_Distance FLOAT,
Walking_Duration FLOAT,
Projected_Time DATETIME,
Projected_Transit_Time DATETIME
);
*/

/*
CREATE TABLE IF NOT EXISTS Taxis
(
tripID INTEGER,
pickup_datetime DATETIME,
pickup_latitude DOUBLE,
pickup_longitude DOUBLE,
dropoff_latitude DOUBLE,
dropoff_longitude DOUBLE,
passenger_count INTEGER,
trip_distance FLOAT,
fare_amount FLOAT,
tip_amount FLOAT,
total_amount FLOAT
);
*/

/*
################################# Insert Green Taxi data into combined taxi data
INSERT INTO Taxis
  SELECT id, STR_TO_DATE(pickup_datetime, '%m/%d/%Y %h:%i:%s %p') as pickup_datetime, pickup_latitude, pickup_longitude, dropoff_latitude, dropoff_longitude, passenger_count, trip_distance, fare_amount, tip_amount, total_amount
  FROM NYC_GreenTaxi2015;
*/

/*
################################# Insert Yellow Cab Taxi data into combined taxi data
INSERT IGNORE
  INTO Taxis 
SELECT id, STR_TO_DATE(pickup_datetime, '%Y-%m-%d %H:%i:%s') as pickup_datetime, pickup_latitude, pickup_longitude, dropoff_latitude, dropoff_longitude, passenger_count, trip_distance, fare_amount, tip_amount, total_amount
  FROM NYC_YellowTaxi2015;
*/

################################# TODO: CREATE TABLE FOR COMBINED TAXI TRIPS

/*
################################# create backup table for Trips
CREATE TABLE TripsBackup LIKE Trips;
INSERT TripsBackup SELECT * FROM Trips;
select * from TripsBackup;
*/

/*
################################# create backup table for NYC_GreenTaxi2015
CREATE TABLE NYC_GreenTaxi2015Backup LIKE NYC_GreenTaxi2015;
INSERT NYC_GreenTaxi2015Backup SELECT * FROM NYC_GreenTaxi2015;
select * from NYC_GreenTaxi2015Backup;
*/

/*
################################# create backup table for NYC_GreenTaxi2015
CREATE TABLE NYC_YellowTaxi2015Backup LIKE NYC_YellowTaxi2015;
INSERT NYC_YellowTaxi2015Backup SELECT * FROM NYC_YellowTaxi2015;
select * from NYC_YellowTaxi2015Backup;
*/

################################# Delete records where Google Maps API query error occured (timeout)
#DELETE FROM Trips WHERE Driving_Distance = 99999;
#select * from Trips;



################################# ADD ID column to taxi tables
#ALTER TABLE NYC_GreenTaxi2015 ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST, AUTO_INCREMENT=1;
#ALTER TABLE NYC_YellowTaxi2015 ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST, AUTO_INCREMENT=1684;
#ALTER TABLE Trips ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST, AUTO_INCREMENT=1;
#ALTER TABLE NYC_GreenTaxi2015Backup ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST, AUTO_INCREMENT=1;
#select * from Trips;