# Requests is a package that allows download of data from any online resource.
# The json_normalize package is to convert data into a pandas DataFrame from a JSON format.
import requests
from pandas.io.json import json_normalize

# Pass the URL to point Python to this location.
r = requests.get("http://www.citibikenyc.com/stations/json")

# To inspect the available methods in the document, use "r.[Tab]"
# To get a basic view of the content in one long string of text, use "r.text"
# To display the values and structure of data, use "r.json()"
# To obtain a list of keys available at the JSON level, use "r.json().keys()"

# Import JSON format station data into a DataFrame (not the whole JSON)
df = json_normalize(r.json()["stationBeanList"])