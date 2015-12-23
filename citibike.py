# ----------------
# IMPORT PACKAGES
# ----------------

# Requests is a package that allows download of data from any online resource.
# The pandas package is used to fetch and store data in a DataFrame.
# The json_normalize package is to convert data into a pandas DataFrame from a JSON format.
# The sqlite3 model is used to work with the SQLite database.
# The time package has datetime objects.
# Dateutil is a package for parsing strings into a Python datetime object.
# The collections module implements specialized container data types.
# The matplotlib package is for graphical outputs (eg. box-plot, histogram, QQ-plot).
import requests
import pandas as pd
from pandas.io.json import json_normalize
import sqlite3 as lite
import time
from dateutil.parser import parse
import collections
import matplotlib.pyplot as plt

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

for t in range(60):
	r = requests.get("http://www.citibikenyc.com/stations/json")
	exec_time = parse(r.json()["executionTime"])

	# Entry for execution times
	with con:
		cur.execute("INSERT INTO available_bikes (execution_time) VALUES (?)", (exec_time.strftime("%Y/%m/%d %H:%M:%S"),))

	id_bikes = collections.defaultdict(int)

	for station in r.json()["stationBeanList"]:
		id_bikes[station["id"]] = station["availableBikes"]

	with con:
		for k, v in id_bikes.iteritems():
			cur.execute("UPDATE available_bikes SET _" + str(k) + " = " + str(v) + " WHERE execution_time = " + exec_time.strftime("'%Y/%m/%d %H:%M:%S'") + ";")

	# Sleep or pause the program for specified number of seconds.
	time.sleep(60)

# Close connection to database
con.close()

# ----------------
# ANALYZE DATA
# ----------------

con = lite.connect("citi_bike.db")
cur = con.cursor()

# Load collected data into a DataFrame and read.
df = pd.read_sql_query("SELECT * FROM available_bikes ORDER BY execution_time", con, index_col="execution_time")

# Create a dictionary of station and their corresponding activity in key-value pairs.
hour_change = collections.defaultdict(int)

# Loop over each station
for col in df.columns:
	# List the values of available bikes for every minute at individual stations 
	station_vals = df[col].tolist()
	# Strip heading underscore "_" from column heading
	station_id = col[1:]
	
	station_change = 0
	for k, v in enumerate(station_vals):
		if k < len(station_vals) - 1:
			station_change += abs(station_vals[k] - station_vals[k + 1])
	hour_change[int(station_id)] = station_change

# Assign key with the highest value to a station
max_station = max(hour_change, key=hour_change.get)

# Query SQLite for information
cur.execute("SELECT id, stationname, latitude, longitude FROM citibike_reference WHERE id = ?", (max_station,))
data = cur.fetchone()

print("The most active station is StationId %s at %s. Coordinates are Latitude: %s and Longitude: %s" % data)
print("with %d bicycles coming and going in the hour between %s and %s." % (
	hour_change[max_station],
	str(df.index[0][11:]),
	str(df.index[-1][11:]),
	))

# ----------------
# VISUALIZE DATA
# ----------------

# Bar Chart of Activity and Station ID
plt.figure()
plt.bar(hour_change.keys(), hour_change.values())
plt.xlabel("Station ID")
plt.ylabel("Activity (Total Number of Bicycles Taken Out or Returned")
plt.title("Activity vs. Station ID")
plt.savefig("bike_activity.png")

# Scatter Plot of Activity and Station Locations
coord = pd.read_sql_query("SELECT longitude, latitude FROM citibike_reference", con)
lon = coord["longitude"].tolist()
lat = coord["latitude"].tolist()
size = [s ** 1.5 for s in hour_change.values()]

plt.figure(figsize=(10, 10))
plt.scatter(lon, lat, s=size, alpha=0.5)
plt.scatter(data[3], data[2], s=hour_change[max_station] ** 1.5, alpha=1, c="r")
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.title("One hour CitiBike activity from " + str(df.index[0][11:]) + " to " + str(df.index[-1][11:]))
plt.savefig("activity_scatter.png")