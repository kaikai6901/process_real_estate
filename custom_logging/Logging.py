from utils.helpers import *
from settings import *
from datetime import datetime, timedelta

class Logging:
    def __init__(self, client) -> None:
        self. client = client
        self.collection = client[DES_DATABASE][LOG_COLLECTION]
        self.runtime_id = datetime.now()

    def log(self, event, message, step):
        log_message  = {
            'event': event,
            'runtime_id': self.runtime_id,
            'createdAt': datetime.now(),
            'step': step
        }
        if isinstance(message, str):
            log_message['message'] = message
        elif isinstance(message, dict):
            for key, value in message.items():
                log_message[key] = value
        self.collection.insert_one(log_message)
