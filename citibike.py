# ----------------
# IMPORT PACKAGES
# ----------------

# Requests is a package that allows download of data from any online resource.
# The json_normalize package is to convert data into a pandas DataFrame from a JSON format.
# The sqlite3 model is used to work with the SQLite database.
import requests
from pandas.io.json import json_normalize
import sqlite3 as lite

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
with con:
	cur.execute("CREATE TABLE citibike_reference (
		id INT PRIMARY KEY,
		totalDocks INT,
		city TEXT,
		altitude INT,
		stAddress2 TEXT,
		longitude NUMERIC,
		postalCode TEXT,
		testStation TEXT,
		stAddress1 TEXT,
		stationName TEXT,
		landMark TEXT,
		latitude NUMERIC,
		location TEXT
		)")