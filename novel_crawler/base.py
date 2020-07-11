import json
import logging
from datetime import datetime
from novel_crawler.utils.func import get_logger


class JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        elif isinstance(obj, bytes):
            return obj.decode('utf8')
        else:
            return json.JSONEncoder.default(self, obj)


class BaseObject(object):
    def __init__(self, log_name='', log_level=logging.INFO):
        self.log = get_logger(log_name, log_level)
