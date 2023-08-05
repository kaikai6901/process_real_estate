from utils.helpers import *
from datetime import datetime, timedelta
from tqdm import tqdm
from custom_logging.Logging import Logging
from settings import *

one_month_ago = datetime.now() - timedelta(days=32)

class ProcessProject:
    def __init__(self, client, logging: Logging ) -> None:
        self.client = client
        self.logging = logging
        self.step = 'process_project'
    
    def calculate_signature_project(self):
        self.logging.log(event='calculate_signature_project', message='start', step=self.step)
        news_collection = self.client[DES_DATABASE][NEWS_COLLECTION]
        base_project_collection = self.client[DES_DATABASE][BASEPROJECT_COLLECTION]

        pipeline = [
            {
                '$match': {
                    'last_time_in_page': { '$gte': one_month_ago }
                }
            },
            {
                '$group': {
                    '_id': '$base_project.project_id',
                    'avg_price_per_m2': { '$avg': '$price_per_m2' },
                    'max_price_per_m2': { '$max': '$price_per_m2' },
                    'min_price_per_m2': { '$min': '$price_per_m2' },
                    'avg_square': { '$avg': '$square' },
                    'max_square': { '$max': '$square' },
                    'min_square': { '$min': '$square' },
                    'avg_total_price': {'$avg': '$total_price'},
                    'max_total_price': { '$max': '$total_price' },
                    'min_total_price': { '$min': '$total_price' },
                    'count': { '$sum': 1 }
                }
            }
        ]
        print('Start load signature')
        result = list(news_collection.aggregate(pipeline))
        self.logging.log(event='calculate_signature_project', message='load signature', step=self.step)
        print('Load done')

        for res in tqdm(result):
            base_project_collection.update_one(
                {'project_id': res['_id']},
                {
                    '$set': {
                        'avg_price_per_m2': res['avg_price_per_m2'],
                        'max_price_per_m2': res['max_price_per_m2'],
                        'min_price_per_m2': res['min_price_per_m2'],
                        'avg_square': res['avg_square'],
                        'max_square': res['max_square'],
                        'min_square': res['min_square'],
                        'avg_total_price': res['avg_total_price'],
                        'max_total_price': res['max_total_price'],
                        'min_total_price': res['min_total_price'],
                        'n_news': res['count']
                    }
                }
            )
        self.logging.log(event='calculate_signature_project', message='end', step=self.step)
        