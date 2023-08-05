from utils.helpers import *
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from unidecode import unidecode
from tqdm import tqdm
from nltk.translate.bleu_score import sentence_bleu, SmoothingFunction
import time
from settings import *
from custom_logging.Logging import Logging
from pymongo.mongo_client import MongoClient
### Add location
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

class ProcesDailyItem():
    def __init__(self, client: MongoClient, logging: Logging) -> None:
        self.client = client
        self.logging = logging
        self.step = 'process_item'

    ### Get news not process
    def get_news(self) -> list:
        self.logging.log(event='get_news', message='start', step=self.step)
        news_collection = self.client[DES_DATABASE][NEWS_COLLECTION]
        item_collection = self.client[RAW_DATABASE][ITEM_COLLECTION]

        ## Get latest process
        max_createdAt = news_collection.find_one({}, sort=[("createdAt", -1)])["createdAt"]
        max_process_id = news_collection.find_one({}, sort=[("process_id", -1)])["process_id"]

        this_process_id = max_process_id + 1

        ### Get data not process
        item_response = item_collection.find({'createdAt': {
            "$gt": max_createdAt
        }})
        raw_data = list(item_response)
        message = {
            'this_process_id': this_process_id,
            'number_news': len(raw_data),
            'message': 'end'
        }
        self.logging.log(event='get_news', message=message, step=self.step)
        return this_process_id, raw_data
    
    ### Read project data
    def get_projects(self):
        self.logging.log(event='get_projects', message='start', step=self.step)
        project_collection = self.client[DES_DATABASE][PROJECT_COLLECTION]
        projects_response = project_collection.find({})

        full_project_df = pd.DataFrame(list(projects_response))
        message = {
            'number_projects': len(full_project_df),
            'message': 'end'
        }
        self.logging.log(event='get_projects', message=message, step=self.step)
        return full_project_df

    ### Remove missing address data
    def remove_missing_address(self, raw_data):
        self.logging.log(event='remove_missing_address', message='start', step=self.step)
        used_data = []  
        trash_data = []
        for news in raw_data:
            if ('project' in news and check_valid_string_field(news['project'])) or ('district' in news and check_valid_string_field(news['district'])):
                used_data.append(news)
            else:
                news['reason'] = "Can't find address from this item"
                trash_data.append(news)
        message = {
            'number_news': len(used_data),
            'message': 'end'
        }
        self.logging.log(event='remove_missing_address', message=message, step=self.step)
        return used_data

    ### Remove missing price or square
    def remove_missing_price(self, used_data):
        self.logging.log(event='remove_missing_price', message='start', step=self.step)
        used_data_no_missing = []
        unused_data = []
        for item in used_data:
            if 'total_price' not in item or 'square' not in item or 'price_per_m2' not in item:
                print(item)
                item['reason'] = 'Missing price'
                unused_data.append(item)
                continue
            price = item['total_price']
            square = item['square']
            price_per_m2 = item['price_per_m2']

            if price == 0 or price is None or square == 0 or square is None or price_per_m2 == 0 or price_per_m2 is None:
                item['reason'] = 'Missing price'
                unused_data.append(item)
                continue
            used_data_no_missing.append(item)
        message = {
            'number_news': len(used_data_no_missing),
            'message': 'end'
        }
        self.logging.log(event='remove_missing_price', message=message, step=self.step)
        return used_data_no_missing

    ### Remove outlier
    def remove_outlier(self, used_data_no_missing):
        self.logging.log(event='remove_outlier', message='start', step=self.step)
        # price_per_m2_values = np.array([d['price_per_m2'] for d in used_data_no_missing])
        # square_values = np.array([d['square'] for d in used_data_no_missing])
        used_data_no_outlier = [d for d in used_data_no_missing if 3e6 < d['price_per_m2'] < 3e8 and d['square'] > 20]
        message = {
            'number_news': len(used_data_no_outlier),
            'message': 'end'
        }
        self.logging.log(event='remove_outlier', message=message, step=self.step)
        return used_data_no_outlier


    ### Remove duplicates
    def remove_inside_duplicates(self,used_data_no_outlier):
        self.logging.log(event='remove_inside_duplicates', message='start', step=self.step)
        duplicate_item = []
        tokens = [
            unidecode(d['title']).strip().lower().split() for d in used_data_no_outlier
        ]
        smoothing_function = SmoothingFunction().method2
        list_unique_item = []
        processed_item = set()
        for i in tqdm(range(len(used_data_no_outlier))):
            item = used_data_no_outlier[i]
            if i in processed_item:
                continue
            processed_item.add(i)
            unique_item = item

            reference_tokens = [tokens[i]]
            for j in range(len(used_data_no_outlier)):
                if j in processed_item:
                    continue
                another_item = used_data_no_outlier[j]
                candidate_tokens = tokens[j]
                if item['square'] == another_item['square'] and is_approximately(unique_item['total_price'], another_item['total_price']):
                    if sentence_bleu(reference_tokens, candidate_tokens, weights=(0.5, 0.5), smoothing_function=smoothing_function) > 0.9:
                        processed_item.add(j)
                        duplicate_item.append([unique_item['news_url'], another_item['news_url']])
                        if unique_item['published_at'] < another_item['published_at']:
                            unique_item = another_item
            list_unique_item.append(unique_item)
        message = {
            'number_news': len(list_unique_item),
            'message': 'end'
        }
        self.logging.log(event='remove_inside_duplicates', message=message, step=self.step)
        return list_unique_item

    def remove_old_duplicates(self, list_unique_item):
        self.logging.log(event='remove_old_duplicates', message='start', step=self.step)
        news_collection = self.client[DES_DATABASE][NEWS_COLLECTION]
    ### Compare to old news in database
        prev_date = datetime.now() - timedelta(days=15)
        old_news = list(news_collection.find({
            'published_at': {
                '$gt': prev_date
            }
            },
            {
                'title': 1,
                'square': 1,
                'total_price': 1
            }
        ))

        tokens = [
            unidecode(d['title']).strip().lower().split() for d in list_unique_item
        ]
        smoothing_function = SmoothingFunction().method2

        old_tokens = [
            d['title'].split() for d in old_news
        ]

        list_unique_item_from_old = []
        for i in tqdm(range(len(list_unique_item))):
            item = list_unique_item[i]

            reference_tokens = [tokens[i]]
            is_duplicate = False
            for j in range(len(old_news)):
                another_item = old_news[j]
                candidate_tokens = old_tokens[j]
                if item['square'] == another_item['square']:
                    if sentence_bleu(reference_tokens, candidate_tokens, weights=(0.5, 0.5), smoothing_function=smoothing_function) > 0.9:
                        # if item['published_at'] - timedelta(days=15) <  another_item['published_at']:
                            is_duplicate = True
                            break
            if not is_duplicate:
                list_unique_item_from_old.append(item)  

        message = {
            'number_news': len(list_unique_item_from_old),
            'message': 'end'
        }
        self.logging.log(event='remove_old_duplicates', message=message, step=self.step)
        return list_unique_item_from_old


    def extract_name_project(self, project: str):
        project = project.strip().lower()
        for kind in type_of_project:
            project = project.replace(kind, '')
        project = project.strip().title()
        return project
    

    def process_project(self,list_unique_item_from_old):
        full_project_df = self.get_projects()
        project_collection = self.client[DES_DATABASE][PROJECT_COLLECTION]
        base_project_collection = self.client[DES_DATABASE][BASEPROJECT_COLLECTION]

        self.logging.log(event='process_project', message='start', step=self.step)
        unseen_project = set()
        for news in tqdm(list_unique_item_from_old):
            if 'project' in news and check_valid_string_field(news['project']):
                name = news['project']
                formatted_address = get_formatted_address(news['commune'], news['district'], news['province'])

                url = None
                if 'project_url' in news and news['project_url'] is not None:
                    domain = list_domain[news['source']]
                    url =  get_absolute_path(news['project_url'], domain)

                reduced_name = self.extract_name_project(name)
                if get_formatted_string(reduced_name) not in formatted_address:
                    name = reduced_name
                match_project = full_project_df[full_project_df['formatted_name'] == get_formatted_string(name)]

                if len(match_project) == 0:
                    query = ', '.join([name, news['district'], news['province']])
                    unseen_project.add((name, news['source'],url, query))
        message = {
            'number_useen_project': len(unseen_project),
            'unseen_project': list(unseen_project)
        }
        self.logging.log(event='process_project', message=message, step=self.step)

        list_unseen_project = []
        parser = GoongParser()
        i = 0
        for prj in unseen_project:
            print(i)
            i += 1
            new_prj = {}
            new_prj['name'] = prj[0]
            new_prj['source'] = prj[1]
            new_prj['url'] = prj[2]
            new_prj['formatted_name'] = get_formatted_string(prj[0])

            try:
                res = parser.parse_address(prj[3])
                new_prj['parser_response'] = res
            except:
                parser._reconnect()
                res = parser.parse_address(prj[3])
                new_prj['parser_response'] = res
            time.sleep(1.5)
            list_unseen_project.append(new_prj)

        self.logging.log(event='process_project', message='retrieve loc from Goong done', step=self.step)
        list_new_project = []
        list_match_project = []
        for new_prj in list_unseen_project:
            name = new_prj['name']
            print(name)
            new_prj['loc'] = {
                'type': 'Point',
                'coordinates': [
                    new_prj['parser_response']['results'][0]['geometry']['location']['lng'],
                    new_prj['parser_response']['results'][0]['geometry']['location']['lat'],
                ]
            }

            new_prj['address'] = {
                'name':  new_prj['parser_response']['results'][0]['name'],
                'address': new_prj['parser_response']['results'][0]['address'],
                'compound':  new_prj['parser_response']['results'][0]['compound'],
                'formatted_compound': get_formatted_compound(new_prj['parser_response']['results'][0]['compound'])
            }
            is_match = False
            for id, row in full_project_df.iterrows():
                if string_equal(name, row['name']):
                    if calculate_distance(new_prj['loc'], row['loc']) < 0.5:
                        print(calculate_distance(new_prj['loc'], row['loc']))
                        new_prj['base_project'] = row['base_project']
                        list_match_project.append(new_prj)
                        is_match = True
                        break
            if not is_match:
                list_new_project.append(new_prj)

        message = {
            'number_match_project': len(list_match_project),
            'match_project': list_match_project,
            'number_new_project': len(list_new_project),
            'new_project': list_new_project
        }
        self.logging.log(event='process_project', message=message, step=self.step)

        if len(list_match_project) > 0:
            project_collection.insert_many(list_match_project)

        if len(list_new_project) > 0:
            result = base_project_collection.find_one({}, sort=[('project_id', -1)], projection={'project_id': 1})
            max_project_id = result['project_id']

            i = 1
            for base in list_new_project:
                base['project_id'] = max_project_id + i
                i += 1


            base_project_collection.insert_many(list_new_project)

            for new_prj in list_new_project:
                prj = {
                    'name': new_prj['name'],
                    'source': new_prj['source'],
                    'url': new_prj['url'],
                    'loc': new_prj['loc'],
                    'parser_response': new_prj['parser_response'],
                    'address': new_prj['address'],
                    'formatted_name': new_prj['formatted_name'],
                    'base_project': {
                        'project_id': new_prj['project_id'],
                        'name': new_prj['name'],
                        'address': new_prj['address'],
                        'loc': new_prj['loc'],
                        'url': new_prj['url'],
                        'source': new_prj['source']
                    }
                }
                project_collection.insert_one(prj)
        self.logging.log(event='process_project', message='end', step=self.step)

    def assign_loc(self, list_unique_item_from_old):
        self.logging.log(event='assign_loc', message='start', step=self.step)

        full_project_df = self.get_projects()

        
        used_data_with_loc = []
        commune_address_collection = self.client[DES_DATABASE][COMMUNE_ADDRESS_COLLECTION]
        for news in tqdm(list_unique_item_from_old):
            if 'project' in news and check_valid_string_field(news['project']):
                name = news['project']
                reduced_name = self.extract_name_project(name)
                formatted_address = get_formatted_address(news['commune'], news['district'], news['province'])
                if get_formatted_string(reduced_name) not in formatted_address:
                    name = reduced_name
                
                name = get_formatted_string(name)

                match_project = full_project_df[full_project_df['formatted_name'] == name]

                if len(match_project) == 0:
                    message = {
                        'name': name,
                        'message': 'have project but not found'
                    }
                    self.logging.log(event='assign_loc', message=message, step=self.step)
                    print(name, 'have project but not found')
                    continue
                
                news['loc'] = match_project.iloc[0]['loc']
                news['base_project'] = match_project.iloc[0]['base_project']
                news['location_confidence'] = 1
            else:
                news['project'] = None
                news['project_url'] = None
                formatted_address = get_formatted_address(news['commune'], news['district'], news['province'])
                query = {
                    'formatted_address': formatted_address
                }

                res = list(commune_address_collection.find(query))
                if len(res) == 0:
                    message = {
                        'address': query,
                        'message': 'address not found'
                    }
                    self.logging.log(event='assign_loc', message=message, step=self.step)
                    print('Address not found')
                    continue
                else:
                    news['loc'] = res[0]['loc']
                    
                if news['commune']:
                    news['location_confidence'] = 0
                else:
                    news['location_confidence'] = -1
                    query_ = {
                        'formatted_compound.district': unidecode(news['district']).strip().lower(),
                        'formatted_compound.province': unidecode(news['province']).strip().lower()
                    }   
                    res_ = list(commune_address_collection.find(query_))
                    news['list_commune_match'] = []
                    for loc in res_:
                        news['list_commune_match'].append(loc['loc'])
            
            used_data_with_loc.append(news)
        message = {
            'number_news': len(used_data_with_loc),
            'message': 'end'
        }
        self.logging.log(event='assign_loc', message=message, step=self.step)
        return used_data_with_loc

    def save_news(self, used_data_with_loc, this_process_id):
        self.logging.log(event='save_news', message='start', step=self.step)
        news_collection = self.client[DES_DATABASE][NEWS_COLLECTION]
        for i in range(len(used_data_with_loc)):
            new = used_data_with_loc[i]
            new['process_id'] = this_process_id
            new['transformAt'] = datetime.now() 
        
        news_collection.insert_many(used_data_with_loc)
        self.logging.log(event='save_news', message='end', step=self.step)
# def is_valid_coordinates(coord):
#     latitude = coord[1]
#     longitude = coord[0]
#     if latitude < -90 or latitude > 90:
#         return False
    
#     if longitude < -180 or longitude > 180:
#         return False
    
#     return True

# for new in used_data_with_loc:
#     coordinates = new['loc']['coordinates']
#     if not is_valid_coordinates(coordinates):
#         print(new)

# news_collection.insert_many(used_data_with_loc)
# client.close()