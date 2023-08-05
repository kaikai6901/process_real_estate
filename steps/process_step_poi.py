from utils.helpers import *
import requests
import json
from unidecode import unidecode
import time 
from tqdm import tqdm
from settings import *
from custom_logging.Logging import Logging


features= {
    "primary_highway": [{
        "key": "highway",
        "value": "primary"
    }],
    "secondary_highway": [{
        "key": "highway",
        "value": "secondary"
    }],
     "tertiary_highway": [{
        "key": "highway",
        "value": "tertiary"
    }],
     "residential_highway": [{
        "key": "highway",
        "value": "residential"
    }],
     "bus_stop": [{
        "key": "highway",
        "value": "bus_stop"
    }],
     "supermarket": [{
        "key": "shop",
        "value": "supermarket"
    }],
     "mall": [{
        "key": "shop",
        "value": "mall"
    }],
     "hospital": [{
        "key": "amenity",
        "value": "hospital"
    }],
    "college": [{
        "key": "amenity",
        "value": "college"
    }],
     "school": [{
        "key": "amenity",
        "value": "school"
    }],
     "university": [{
        "key": "amenity",
        "value": "university"
    }],
}

distances = {
    'amenities_in_500': 500,
    'amenities_in_1000': 1000,
    'amenities_in_3000': 2000
}
def find_nearby_way(latitude, longitude, key, value, distance):
    overpass_url = "http://overpass-api.de/api/interpreter"
    
    overpass_query = """
        [out:json];
        (
            way["{key}"="{value}"](around:{distance}, {lat}, {lon});
            node["{key}"="{value}"](around:{distance}, {lat}, {lon});
            relation["{key}"="{value}"](around:{distance}, {lat}, {lon});
        );
        out center;
    """.format(lat=latitude, lon=longitude, key=key, value=value, distance=distance)
    
    response = requests.get(overpass_url, params={'data': overpass_query})
    data = response.json()
    return data

def compare_string(a: str, b: str):
    formatted_a = unidecode(a).strip().lower()
    formatted_b = unidecode(b).strip().lower()

    if a == b: 
        return True
    words_a = formatted_a.split(' ')
    words_b = formatted_b.split(' ')

    for i in range(len(words_a) - 1):
        if words_a[i] + ' ' + words_a[i + 1] in formatted_b:
            return True
    for i in range(len(words_b) - 1):
        if words_b[i] + ' ' + words_b[i + 1] in formatted_a:
            return True

    return False

class ProcessPoi:
    def __init__(self, client, logging: Logging) -> None:
        self.client = client
        self.logging = logging
        self.step = 'process_poi'
    
    def get_projects(self):
        print('Start load project')
        self.logging.log(event='get_projects', message='start', step=self.step)
        base_project_collection = self.client[DES_DATABASE][BASEPROJECT_COLLECTION]
        base_projects = list(base_project_collection.find({'amenities': {'$exists': False}}, {'parser_response': 0}))
        message = {
            'number_new_projects': len(base_projects),
            'message': 'end'
        }
        self.logging.log(event='get_projects', message=message, step=self.step)
        print('Load project done!!!')
        return base_projects
    
    def get_poi(self, base_projects):
        self.logging.log(event='get_poi', message='start', step=self.step)
        base_project_collection = self.client[DES_DATABASE][BASEPROJECT_COLLECTION]
        i = 0
        for prj in tqdm(base_projects):
            if 'amenities' in prj:
                print(i)
                i += 1
                continue
            print(prj['name'])
            loc = prj['loc']['coordinates']
            amenities = {}
            amenities_detail = {}

            for key, dist in distances.items():
                amenities[key] = {}
                amenities_detail[key] = {}

                for feature, tags in features.items():
                    tag = tags[0]
                    results = find_nearby_way(loc[1], loc[0], key=tag['key'], value=tag['value'], distance=dist)
                    res  = []
                    set_name = set()

                    for element in results['elements']:
                        if 'name' not in element['tags']:
                            continue

                        name = element['tags']['name']
                        if name in set_name:
                            continue

                        vocab = ['ngõ', 'hẻm', 'ngách']
                        for word in vocab:
                            if word in name.lower():
                                continue
                        
                        is_duplicate = False
                        if 'highway' in feature:
                            for old_name in set_name:
                                if compare_string(old_name, name):
                                    is_duplicate=True
                                    break
                        
                        if not is_duplicate:
                            set_name.add(name)
                            res.append(element)
                    
                    amenities[key][feature] = len(res)
                    amenities_detail[key][feature] = res
                    time.sleep(2)

            base_project_collection.update_one({'_id': prj['_id']}, {'$set': {'amenities': amenities, 'amenities_detail': amenities_detail}})
            print(i)
            i += 1
        self.logging.log(event='get_poi', message='end', step=self.step)
    


        
# client = get_mongodb_client()

# DATABASE = 'real_estate'
# PROJECT_COLLECTION = 'base_project'

# project_collection = client[DATABASE][PROJECT_COLLECTION]

# print('Start load project')
# base_projects = list(project_collection.find({'amenities': {'$exists': False}}, {'parser_response': 0}))


# i = 0
# for prj in tqdm(base_projects):
#     if 'amenities' in prj:
#         print(i)
#         i += 1
#         continue
#     print(prj['name'])
#     loc = prj['loc']['coordinates']
#     amenities = {}
#     amenities_detail = {}

#     for key, dist in distances.items():
#         amenities[key] = {}
#         amenities_detail[key] = {}

#         for feature, tags in features.items():
#             tag = tags[0]
#             results = find_nearby_way(loc[1], loc[0], key=tag['key'], value=tag['value'], distance=dist)
#             res  = []
#             set_name = set()

#             for element in results['elements']:
#                 if 'name' not in element['tags']:
#                     continue

#                 name = element['tags']['name']
#                 if name in set_name:
#                     continue

#                 vocab = ['ngõ', 'hẻm', 'ngách']
#                 for word in vocab:
#                     if word in name.lower():
#                         continue
                
#                 is_duplicate = False
#                 if 'highway' in feature:
#                     for old_name in set_name:
#                         if compare_string(old_name, name):
#                             is_duplicate=True
#                             break
                
#                 if not is_duplicate:
#                     set_name.add(name)
#                     res.append(element)
            
#             amenities[key][feature] = len(res)
#             amenities_detail[key][feature] = res
#             time.sleep(2)

#     project_collection.update_one({'_id': prj['_id']}, {'$set': {'amenities': amenities, 'amenities_detail': amenities_detail}})
#     print(i)
#     i += 1

# client.close()
# print('DONE!!!!!')