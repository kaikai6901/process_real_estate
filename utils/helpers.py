import pymongo
import configparser
import re
import http.client, urllib.parse
import json
import urllib
from unidecode import unidecode
import math

CONFIG_PATH = 'configs/configs.cfg'
MONGODB_CONFIG_NAME = 'MongoDB'
GOONG_CONGIG_NAME = 'GoongAPI'

config = configparser.ConfigParser()
config.read(CONFIG_PATH)

special_character_pattern = "[" + re.escape("!@#$%^&*()-_+={}[]|\\/:;\"',.?`~") + "]"
def get_mongodb_client():
    host = config.get(MONGODB_CONFIG_NAME, 'host')
    user = config.get(MONGODB_CONFIG_NAME, 'user')
    password = config.get(MONGODB_CONFIG_NAME, 'password')
    mongo_uri = f'mongodb+srv://{user}:{password}@{host}/?retryWrites=true&w=majority'
    return pymongo.MongoClient(mongo_uri, retryWrites=False)

def check_valid_string_field(s):
    if s is None:
            return False
    if not isinstance(s, str):
        return False
    s_clean = re.sub(special_character_pattern, '', s)
    
    if s_clean:
        return True
    return False

def is_approximately(num1, num2):
    # Calculate the difference between the two numbers
    difference = abs(num1 - num2)
    
    # Calculate the percentage difference
    percent_difference = (difference / num1) * 100
    
    # Check if the percentage difference is less than 5%
    if percent_difference < 5:
        return True
    else:
        return False
    

def get_absolute_path(url, domain):
    return urllib.parse.urljoin(domain, url)

def get_formatted_address(commune, district, province):
    address = []
    if commune:
        address.append(unidecode(commune).strip().lower())
    address.append(unidecode(district).strip().lower())
    address.append(unidecode(province).strip().lower())
    return ', '.join(address)

def get_formatted_string(s: str):
    if s is None:
        return None
    return unidecode(s).strip().lower()

def string_equal(sentences_1, sentences_2):
    sentence1_clean = re.sub(special_character_pattern, '', sentences_1)
    sentence2_clean = re.sub(special_character_pattern, '', sentences_2)

    sentence1_clean = unidecode(sentence1_clean).strip().lower()
    sentence2_clean = unidecode(sentence2_clean).strip().lower()
    # Split sentences into words
    words1 = sentence1_clean.split()
    words2 = sentence2_clean.split()
    return all(word in sentence2_clean for word in words1) or all(word in sentence1_clean for word in words2)

def calculate_distance(loc1, loc2):
    # Convert latitude and longitude from degrees to radians

    lat1 = loc1['coordinates'][0]
    lon1 = loc1['coordinates'][1]
    lat2 = loc2['coordinates'][0]
    lon2 = loc2['coordinates'][1]
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # Radius of the Earth in kilometers
    radius = 6371

    # Difference between the latitudes and longitudes
    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad

    # Haversine formula
    a = math.sin(delta_lat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    # Calculate the distance
    distance = radius * c

    return distance

def get_formatted_compound(compound):
    if compound is None:
        return None
    formatted_compound = {}
    for key, value in compound.items():
        formatted_compound[key] = get_formatted_string(value)
    return formatted_compound

class GoongParser:
    _instance = None
    _connection = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(GoongParser, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        if not self._connection:
            self._connection = self._connect_to_server()

    def _connect_to_server(self):
        # Implement your connection logic here
        # This could involve creating a network connection, authenticating, etc.
        conn = http.client.HTTPSConnection('rsapi.goong.io')
        return conn

    def _reconnect(self):
        self._connection = self._connect_to_server()
        
    def parse_address(self, address):
        params = urllib.parse.urlencode({
            'address': address,
            'api_key': config.get(GOONG_CONGIG_NAME, 'key')
        })
        # Implement the geocoding logic using the server connection
        self._connection.request('GET', '/Geocode?{}'.format(params))

        res = self._connection.getresponse()
        data = res.read()
        return json.loads(data)

    def get_connection(self):
        return self._connection
    
    def finalize(self):
        # Implement any cleanup logic here (e.g., closing the connection)
        self._connection.close()


