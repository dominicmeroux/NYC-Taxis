########################################
# Goal: run to periodically back up data, since Google Maps API data would take time to replicate
########################################

import csv
import mysql.connector
from mysql.connector import errorcode

# MySQL Database
cnx = mysql.connector.connect(user='MYSQL_USERNAME', password='MYSQL_PASSWORD', host='127.0.0.1')
cursor = cnx.cursor()

# Approach / Code modified from MySQL Connector web page
DB_NAME = "EquityDB"

# 1) Try connecting to the database with given DB_NAME
# 2) If that fails, create database
def create_database(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

try:
    cnx.database = DB_NAME    
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor)
        cnx.database = DB_NAME
    else:
        print(err)
        exit(1)

########################################
# Extract all tables from database
########################################

# NYC Green Taxi table
cursor.execute("select * from NYC_GreenTaxi2015")
NYC_GreenTaxi2015 = cursor.fetchall()
with open("NYC_GreenTaxi2015.csv", "wb") as GreenTaxiFile:
	csv_writer = csv.writer(GreenTaxiFile)
	for i in NYC_GreenTaxi2015:
		csv_writer.writerow(i)

# NYC Yellow Taxi table
cursor.execute("select * from NYC_YellowTaxi2015")
NYC_YellowTaxi2015 = cursor.fetchall()
with open("NYC_YellowTaxi2015.csv", "wb") as YellowTaxiFile:
	csv_writer = csv.writer(YellowTaxiFile)
	for i in NYC_YellowTaxi2015:
		csv_writer.writerow(i)

# Taxis (combined NYC Green and NYC Yellow) table
cursor.execute("select * from Taxis")
Taxis = cursor.fetchall()
with open("Taxis.csv", "wb") as TaxiFile:
	csv_writer = csv.writer(TaxiFile)
	for i in Taxis:
		csv_writer.writerow(i)

# Trips table
cursor.execute("select * from Trips")
Trips = cursor.fetchall()
with open("Trips.csv", "wb") as TripsFile:
	csv_writer = csv.writer(TripsFile)
	for i in Trips:
		csv_writer.writerow(i)

# Close out
cursor.close()
cnx.close()