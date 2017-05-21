# Python libraries
import json
import urllib2
import base64
import time
from urllib2 import urlopen, Request
import re
import datetime
from datetime import datetime, timedelta

# MySQL
import mysql.connector
from mysql.connector import errorcode

# Google Maps API
# Console https://console.developers.google.com/apis/dashboard?project=firstproject-960&duration=PT1H
import googlemaps
gKey = googlemaps.Client(key='INSERT_GOOGLE_API_KEY_HERE')

# Size of 2015 NYC Taxi sample dataset
TotalSampleSize = 8550405

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

########################## NYC Green Taxi 2015
# Pickup Time
cursor.execute("select STR_TO_DATE(pickup_datetime, '%m/%d/%Y %h:%i:%s %p') from NYC_GreenTaxi2015")
pickup_time_NYC_GreenTaxi2015 = cursor.fetchall()
pickup_time = [i[0] for i in pickup_time_NYC_GreenTaxi2015]

# Pickup Latitude
cursor.execute("select pickup_latitude from NYC_GreenTaxi2015")
pickup_latitude_NYC_GreenTaxi2015 = cursor.fetchall()
pickup_latitude = [i[0] for i in pickup_latitude_NYC_GreenTaxi2015]

# Pickup Longitude
cursor.execute("select pickup_longitude from NYC_GreenTaxi2015")
pickup_longitude_NYC_GreenTaxi2015 = cursor.fetchall()
pickup_longitude = [i[0] for i in pickup_longitude_NYC_GreenTaxi2015]

# Dropoff Latitude
cursor.execute("select dropoff_latitude from NYC_GreenTaxi2015")
dropoff_latitude_NYC_GreenTaxi2015 = cursor.fetchall()
dropoff_latitude = [i[0] for i in dropoff_latitude_NYC_GreenTaxi2015]

# Dropoff Longitude
cursor.execute("select dropoff_longitude from NYC_GreenTaxi2015")
dropoff_longitude_NYC_GreenTaxi2015 = cursor.fetchall()
dropoff_longitude = [i[0] for i in dropoff_longitude_NYC_GreenTaxi2015]

########################## NYC Yellow Cab 2015
# Pickup Time
cursor.execute("select STR_TO_DATE(pickup_datetime, '%Y-%m-%d %H:%i:%s') from NYC_YellowTaxi2015")
pickup_time_NYC_YellowTaxi2015 = cursor.fetchall()
pickup_time_NYC_YellowTaxi2015 = [i[0] for i in pickup_time_NYC_YellowTaxi2015]
for i in range(0, len(pickup_time_NYC_YellowTaxi2015)):
    pickup_time.append(pickup_time_NYC_YellowTaxi2015[i])

# Pickup Latitude
cursor.execute("select pickup_longitude from NYC_YellowTaxi2015")
pickup_longitude_NYC_YellowTaxi2015 = cursor.fetchall()
pickup_longitude_NYC_YellowTaxi2015 = [i[0] for i in pickup_longitude_NYC_YellowTaxi2015]
for i in range(0, len(pickup_longitude_NYC_YellowTaxi2015)):
    pickup_longitude.append(pickup_longitude_NYC_YellowTaxi2015[i])

# Pickup Longitude
cursor.execute("select pickup_latitude from NYC_YellowTaxi2015")
pickup_latitude_NYC_YellowTaxi2015 = cursor.fetchall()
pickup_latitude_NYC_YellowTaxi2015 = [i[0] for i in pickup_latitude_NYC_YellowTaxi2015]
for i in range(0, len(pickup_latitude_NYC_YellowTaxi2015)):
    pickup_latitude.append(pickup_latitude_NYC_YellowTaxi2015[i])

# Dropoff Latitude
cursor.execute("select dropoff_longitude from NYC_YellowTaxi2015")
dropoff_longitude_NYC_YellowTaxi2015 = cursor.fetchall()
dropoff_longitude_NYC_YellowTaxi2015 = [i[0] for i in dropoff_longitude_NYC_YellowTaxi2015]
for i in range(0, len(dropoff_longitude_NYC_YellowTaxi2015)):
    dropoff_longitude.append(dropoff_longitude_NYC_YellowTaxi2015[i])

# Dropoff Longitude
cursor.execute("select dropoff_latitude from NYC_YellowTaxi2015")
dropoff_latitude_NYC_YellowTaxi2015 = cursor.fetchall()
dropoff_latitude_NYC_YellowTaxi2015 = [i[0] for i in dropoff_latitude_NYC_YellowTaxi2015]
for i in range(0, len(dropoff_latitude_NYC_YellowTaxi2015)):
    dropoff_latitude.append(dropoff_latitude_NYC_YellowTaxi2015[i])

import datetime

# Project roughly 3 years ahead for non-transit modes, from 2015 to 2018
pickup_future = [i + datetime.timedelta(weeks=52*3) for i in pickup_time]

# Transit doesn't allow for projections far into the future - here I add the number of weeks between the original date
# and the present plus two weeks, in order to preserve the weekday, which should be all that matters in transit time
pickup_future_transit = [i + datetime.timedelta(weeks = ((datetime.datetime.now() - i).days/7) + 2) 
                         for i in pickup_time]

cursor.execute("select count(*) from Trips")
Count = cursor.fetchall()
Count = Count[0][0]

ORIGINS = []
DESTINATIONS = []
TIME = []
TRANSIT_TIME = []
if (Count + (2500/4) < TotalSampleSize):
    for i in range(Count, Count + (2500/4)):
        if (pickup_longitude[i] != 0 and pickup_latitude[i] != 0 and 
            dropoff_longitude[i] != 0 and dropoff_latitude[i] != 0):
            ORIGINS.append(str(pickup_latitude[i])+","+str(pickup_longitude[i]))
            DESTINATIONS.append(str(dropoff_latitude[i])+","+str(dropoff_longitude[i]))
            TIME.append(pickup_future[i])
            TRANSIT_TIME.append(pickup_future_transit[i])
    print "Querying for entries " + str(Count) + " to " + str(Count + (2500/4))
else:
    for i in range(Count, TotalSampleSize):
        if (pickup_longitude[i] != 0 and pickup_latitude[i] != 0 and 
            dropoff_longitude[i] != 0 and dropoff_latitude[i] != 0):
            ORIGINS.append(str(pickup_latitude[i])+","+str(pickup_longitude[i]))
            DESTINATIONS.append(str(dropoff_latitude[i])+","+str(dropoff_longitude[i]))
            TIME.append(pickup_future[i])
            TRANSIT_TIME.append(pickup_future_transit[i])
    print str(2500 - (TotalSampleSize - Count))+" queries remaining for today."

DRIVING_DISTANCES = []
TRANSIT_DISTANCES = []
CYCLING_DISTANCES = []
WALKING_DISTANCES = []

# Track number of instances where Google Maps API faults for each mode
FaultyDrivingCount = 0
FaultyTransitCount = 0
FaultyCyclingCount = 0
FaultyWalkingCount = 0
QueryTrackingCount = 0

for i in range(0, len(ORIGINS)):
    try:
        DRIVING_DISTANCES.append(gKey.distance_matrix(ORIGINS[i], DESTINATIONS[i], mode="driving", departure_time=TIME[i]))
    except Exception as e:
        print e
        DRIVING_DISTANCES.append(99999)
        FaultyDrivingCount += 1
        continue
    try:
        TRANSIT_DISTANCES.append(gKey.distance_matrix(ORIGINS[i], DESTINATIONS[i], mode="transit", departure_time=TRANSIT_TIME[i]))
    except Exception as e:
        print e
        TRANSIT_DISTANCES.append(99999)
        FaultyTransitCount += 1
        continue
    try:
        CYCLING_DISTANCES.append(gKey.distance_matrix(ORIGINS[i], DESTINATIONS[i], mode="bicycling", departure_time=TIME[i]))
    except Exception as e:
        print e
        CYCLING_DISTANCES.append(99999)
        FaultyCyclingCount += 1
        continue
    try:
        WALKING_DISTANCES.append(gKey.distance_matrix(ORIGINS[i], DESTINATIONS[i], mode="walking", departure_time=TIME[i]))
    except Exception as e:
        print e
        WALKING_DISTANCES.append(99999)
        FaultyWalkingCount += 1
        continue
    # Track where we're at in the process by printing a status update every 100 queries
    QueryTrackingCount += 1
    if ((QueryTrackingCount % 100) == 0):
        print str(QueryTrackingCount) + " Google Maps API queries have been completed."

print "Driving API faults: " + str(FaultyDrivingCount)
print "Transit API faults: " + str(FaultyTransitCount)
print "Cycling API faults: " + str(FaultyCyclingCount)
print "Walking API faults: " + str(FaultyWalkingCount)
print "Google Maps API query complete...processing for MySQL"

DRIVING_DISTANCE = []
DRIVING_DURATION = []
DRIVING_DURATION_IN_TRAFFIC = []

TRANSIT_DISTANCE = []
TRANSIT_DURATION = []

CYCLING_DISTANCE = []
CYCLING_DURATION = []

WALKING_DISTANCE = []
WALKING_DURATION = []

for i in range(0, len(DRIVING_DISTANCES)):
    try:
        DRIVING_DISTANCE.append(DRIVING_DISTANCES[i]['rows'][0]['elements'][0]['distance']['value'])
    except:
        DRIVING_DISTANCE.append(99999)
    try:
        DRIVING_DURATION.append(DRIVING_DISTANCES[i]['rows'][0]['elements'][0]['duration']['value'])
    except:
        DRIVING_DURATION.append(99999)
    try:
        DRIVING_DURATION_IN_TRAFFIC.append(DRIVING_DISTANCES[i]['rows'][0]['elements'][0]['duration_in_traffic']['value'])
    except:
        DRIVING_DURATION_IN_TRAFFIC.append(99999)
    try:
        TRANSIT_DISTANCE.append(TRANSIT_DISTANCES[i]['rows'][0]['elements'][0]['distance']['value'])
    except:
        TRANSIT_DISTANCE.append(99999)
    try:
        TRANSIT_DURATION.append(TRANSIT_DISTANCES[i]['rows'][0]['elements'][0]['duration']['value'])
    except:
        TRANSIT_DURATION.append(99999)
    try:
        CYCLING_DISTANCE.append(CYCLING_DISTANCES[i]['rows'][0]['elements'][0]['distance']['value'])
    except:
        CYCLING_DISTANCE.append(99999)
    try:
        CYCLING_DURATION.append(CYCLING_DISTANCES[i]['rows'][0]['elements'][0]['duration']['value'])
    except:
        CYCLING_DURATION.append(99999)
    try:
        WALKING_DISTANCE.append(WALKING_DISTANCES[i]['rows'][0]['elements'][0]['distance']['value'])
    except:
        WALKING_DISTANCE.append(99999)
    try:
        WALKING_DURATION.append(WALKING_DISTANCES[i]['rows'][0]['elements'][0]['duration']['value'])
    except:
        WALKING_DURATION.append(99999)

TripValues = []
Index = []
ORIGIN_Latitude = []
ORIGIN_Longitude = []
DESTINATION_Latitude = []
DESTINATION_Longitude = []
TIME_for_MySQL = []
TRANSIT_TIME_for_MySQL = []

for i in range(0, len(DRIVING_DISTANCE)):
    try:
        Index.append(Count + i)
        ORIGIN_Latitude.append(float(re.sub(r'.*,', "", ORIGINS[i])))
        ORIGIN_Longitude.append(float(re.sub(r',.*', "", ORIGINS[i])))
        DESTINATION_Latitude.append(float(re.sub(r'.*,', "", DESTINATIONS[i])))
        DESTINATION_Longitude.append(float(re.sub(r',.*', "", DESTINATIONS[i])))
        if (DRIVING_DISTANCE[i] != 99999 or TRANSIT_DISTANCE[i] != 99999 or CYCLING_DISTANCE[i] != 99999 or WALKING_DISTANCE[i] != 99999):
            TripValues.append((Index[i], ORIGIN_Latitude[i], ORIGIN_Longitude[i], DESTINATION_Latitude[i], DESTINATION_Longitude[i],
                DRIVING_DISTANCE[i], DRIVING_DURATION[i], DRIVING_DURATION_IN_TRAFFIC[i],
                TRANSIT_DISTANCE[i], TRANSIT_DURATION[i], CYCLING_DISTANCE[i], CYCLING_DURATION[i],
                WALKING_DISTANCE[i], WALKING_DURATION[i], TIME[i], TRANSIT_TIME[i]))
    except:
        break

# Insert data into MySQL table Trips
cursor.executemany("INSERT INTO Trips (Index_ID, ORIGIN_Latitude, ORIGIN_Longitude, DESTINATION_Latitude, DESTINATION_Longitude, Driving_Distance, Driving_Duration, Driving_Duration_in_Traffic, Transit_Distance, Transit_Duration, Cycling_Distance, Cycling_Duration, Walking_Distance, Walking_Duration, Projected_Time, Projected_Transit_Time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", 
                   TripValues)
cnx.commit()

print "Inserted " + str(len(DRIVING_DISTANCE)) + " trip results into database " + DB_NAME

cursor.close()
cnx.close()