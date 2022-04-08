import os
from pathlib import Path
import json
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import csv
import logging
from enum import Enum

logger = logging.getLogger(__name__)

class DataBaseType(Enum):
    DATABASE = 'DATABASE' # init_database(database_type=DataBaseType.DATABASE, file_name=site_name, fields=GlobalWifi)
    JSON = 'JSON' #  init_database(database_type=DataBaseType.CSV, file_name=site_name, fields=['title', 'id'])
    CSV = 'CSV' # init_database(database_type=DataBaseType.JSON, file_name=site_name, fields=None)

def init_database(
        database_type: DataBaseType=DataBaseType.DATABASE, 
        file_name: str='',
        fields=None,
        path=os.getcwd(), 
    ):
    if database_type is DataBaseType.DATABASE and fields and not isinstance(fields, list):
        database = DatabaseUtil(table=fields, path=path, file_name=file_name)
    
    elif database_type is DataBaseType.JSON and not fields:
        database = JsonUtil(path=path, file_name=file_name)
    
    elif database_type is DataBaseType.CSV and fields and isinstance(fields, list):
        database = CsvUtil(path=path, file_name=file_name, field_names=fields)
    
    return database

class DatabaseUtil:
    
    def __init__(self, table, path='', file_name=''):
        self.table = table
        self.path = path + '/data'
        self.file_name = file_name + "_{:%Y-%m-%d_%H-%M-%S}".format(datetime.now()) + ".sqlite3"

        try:
            if not os.path.exists(self.path):
                os.makedirs(self.path)
            Path(self.path + '/' + self.file_name).touch()
            self.engine = create_engine('sqlite:///{}'.format(self.path + '/' + self.file_name))
            self.table.metadata.create_all(self.engine)
        except Exception as error:
            logger.exception(error)
            exit()

    @property
    def session(self):
        Session = sessionmaker(bind=self.engine)
        _session = Session()
        return _session

    def save(self, data):
        try:
            session = self.session
            session.bulk_insert_mappings(self.table, data)
            session.commit()
        except Exception as error:
            logger.exception(error)
        finally:
            session.close()

class JsonUtil:
    def __init__(self, path='', file_name=''):
        self.path = path + '/data'
        self.file_name = file_name + "_{:%Y-%m-%d_%H-%M-%S}".format(datetime.now())

        try:
            if not os.path.exists(self.path):
                os.makedirs(self.path)
        except Exception as error:
            logger.exception(error)
            exit()

    def save(self, data):
        origin_data = []
        
        try:
            with open(self.path + '/' + self.file_name + '.json', 'r') as json_file:
                origin_data = json.load(json_file)
        except Exception as error:
            pass
        
        with open(self.path + '/' + self.file_name + '_tmp.json', 'w', encoding='utf-8') as json_file:
            try:
                origin_data.extend(data)
                json.dump(origin_data, json_file, ensure_ascii=False)
                os.rename(self.path + '/' + self.file_name + '_tmp.json', self.path + '/' + self.file_name + '.json')
            except Exception as error:
                logger.exception(error)

class CsvUtil:
    def __init__(self, path='', file_name='', field_names=[]):
        self.path = path + '/data'
        self.file_name = file_name + "_{:%Y-%m-%d_%H-%M-%S}".format(datetime.now())
        self.field_names = field_names

        try:
            if not os.path.exists(self.path):
                os.makedirs(self.path)
        except Exception as error:
            logger.exception(error)
            exit()

    def save(self, data):
        origin_data = []
        
        try:
            with open(self.path + '/' + self.file_name + '.csv', 'r', newline='') as csv_file:
                rows = csv.DictReader(csv_file)
                origin_data = list(rows)
        except Exception as error:
            pass
        
        with open(self.path + '/' + self.file_name + '_tmp.csv', 'w', newline='') as csv_file:
            try:
                origin_data.extend(data)
                writer = csv.DictWriter(csv_file, fieldnames=self.field_names)
                writer.writeheader()
                writer.writerows(origin_data)
                os.rename(self.path + '/' + self.file_name + '_tmp.csv', self.path + '/' + self.file_name + '.csv')
            except Exception as error:
                logger.exception(error)