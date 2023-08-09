from utils.helpers import *
from steps.process_step_item import *
from steps.process_step_poi import *
from steps.process_step_project import *
from custom_logging.Logging import Logging

def process():
    client = get_mongodb_client()
    logging = Logging(client=client)
    item_process = ProcesDailyItem(client=client, logging=logging)
    item_process.update_last_time_in_page()
    this_process_id, raw_data = item_process.get_news()
    used_data = item_process.remove_missing_address(raw_data=raw_data)
    used_data_no_missing = item_process.remove_missing_price(used_data=used_data)
    used_data_no_outlier = item_process.remove_outlier(used_data_no_missing=used_data_no_missing)
    list_unique_item = item_process.remove_inside_duplicates(used_data_no_outlier)
    list_unique_item_from_old = item_process.remove_old_duplicates(list_unique_item)
    item_process.process_project(list_unique_item_from_old)
    used_data_with_loc = item_process.assign_loc(list_unique_item_from_old)
    item_process.save_news(used_data_with_loc, this_process_id)
    logging.log(event='finished', message='done', step='process_item')

    process_poi = ProcessPoi(client=client, logging=logging)
    base_projects = process_poi.get_projects()
    process_poi.get_poi(base_projects=base_projects)
    
    proces_project = ProcessProject(client=client, logging=logging)
    proces_project.calculate_signature_project()
    client.close()

if __name__ == '__main__':
    process()
    


