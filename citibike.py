# ----------------
# IMPORT PACKAGES
# ----------------

# Requests is a package that allows download of data from any online resource.
# The json_normalize package is to convert data into a pandas DataFrame from a JSON format.
# The sqlite3 model is used to work with the SQLite database.
# The time package has datetime objects.
# Dateutil is a package for parsing strings into a Python datetime object.
# The collections module implements specialized container data types.
import requests
from pandas.io.json import json_normalize
import sqlite3 as lite
import time
from dateutil.parser import parse
import collections

# ----------------
# OBTAIN DATA
# ----------------

# Pass the URL to point Python to this location.
r = requests.get("http://www.citibikenyc.com/stations/json")

# To inspect the available methods in the document, use "r.[Tab]"
# To get a basic view of the content in one long string of text, use "r.text"
# To display the values and structure of data, use "r.json()"
# To obtain a list of keys available at the JSON level, use "r.json().keys()"

# Import JSON format station data into a DataFrame (not the whole JSON)
df = json_normalize(r.json()["stationBeanList"])

# ----------------
# STORE DATA (STATIC)
# ----------------

# Connect to the database. The "connect()" method returns a connection object.
con = lite.connect("citi_bike.db")
cur = con.cursor()

# Drop currently existing tables.
with con:
	cur.execute("DROP TABLE IF EXISTS citibike_reference")
	cur.execute("DROP TABLE IF EXISTS available_bikes")

# Create the table specifying the name of columns and their data types.
# Information that remains static with the station ID (id) number as key value.
with con:
	cur.execute("CREATE TABLE citibike_reference ("
		"id INT PRIMARY KEY, "
		"totalDocks INT, "
		"city TEXT, "
		"altitude INT, "
		"stAddress2 TEXT, "
		"longitude NUMERIC, "
		"postalCode TEXT, "
		"testStation TEXT, "
		"stAddress1 TEXT, "
		"stationName TEXT, "
		"landMark TEXT, "
		"latitude NUMERIC, "
		"location TEXT "
		")")

	# SQL statement to be executed for the row data entry.
	sql = ("INSERT INTO citibike_reference (id, totalDocks, city, "
		"altitude, stAddress2, longitude, postalCode, testStation, "
		"stAddress1, stationName, landMark, latitude, location) "
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

	# For loop to populate values into the database.
	for station in r.json()["stationBeanList"]:
		data = (
			station["id"],
			station["totalDocks"],
			station["city"],
			station["altitude"],
			station["stAddress2"],
			station["longitude"],
			station["postalCode"],
			station["testStation"],
			station["stAddress1"],
			station["stationName"],
			station["landMark"],
			station["latitude"],
			station["location"]
			)
		cur.execute(sql, data)

# To get multiple readings per period, the table needs to be different.
# Extract the ID column from DataFrame to be used in a list.
station_ids = df["id"].tolist()

# Add Underscore "_" to the station name and data type for SQLite
station_ids = ["_" + str(x) + " INT" for x in station_ids]

# Create the table by concatenating the string and joining station IDs.
with con:
	cur.execute("CREATE TABLE available_bikes (execution_time INT, " + ", ".join(station_ids) + ");")

# ----------------
# STORE DATA (DYNAMIC)
# ----------------

r = requests.get("http://www.citibikenyc.com/stations/json")
exec_time = parse(r.json()["executionTime"])

# Entry for execution times
with con:
	cur.execute("INSERT INTO available_bikes (execution_time) VALUES (?)", (exec_time.strftime("'%Y/%m/%d %H:%M:%S'"),))

id_bikes = collections.defaultdict(int)

for station in r.json()["stationBeanList"]:
	id_bikes[station["id"]] = station["availableBikes"]

# Seems to be have a problem in updating the values for each station ID.
# UPDATE available_bikes SET _72 = 10 or SET _72 = '10' works in updating but does not work if the WHERE condition is applied.
# Need to fix.

with con:
	for k, v in id_bikes.iteritems():
		cur.execute("UPDATE available_bikes SET _" + str(k) + " = " + str(v) + " WHERE execution_time = " + exec_time.strftime("'%Y/%m/%d %H:%M:%S'") + ";")