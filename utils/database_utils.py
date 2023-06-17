import logging
import os
from pathlib import Path
import json
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import csv
from enum import Enum
from typing import Optional, Union, List, Dict, Any
from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm.session import Session

logger = logging.getLogger('database')

class DataBaseType(Enum):
    DATABASE = 'DATABASE' # init_database(database_type=DataBaseType.DATABASE, site_name=site_name, fields=GlobalWifi)
    JSON = 'JSON' # init_database(database_type=DataBaseType.JSON, site_name=site_name, fields=None)
    CSV = 'CSV' #  init_database(database_type=DataBaseType.CSV, site_name=site_name, fields=['title', 'id'])

class DatabaseUtil:
    def __init__(self, table: DeclarativeMeta, file_path: str=''):
        self.extension = '.sqlite3'
        self.table = table
        self.file_path = file_path

        try:
            Path(self.file_path + self.extension).touch()
            self.engine: Engine = create_engine('sqlite:///{}'.format(self.file_path + self.extension))
            self.table.metadata.create_all(self.engine)
        except Exception as error:
            logger.critical(error)
            exit()

    @property
    def session(self) -> Session:
        Session = sessionmaker(bind=self.engine)
        _session = Session()
        return _session

    def save(self, data: List[Dict[str, Any]]):
        try:
            session = self.session
            session.bulk_insert_mappings(self.table, data)
            session.commit()
        except Exception as error:
            logger.error(error)
        finally:
            session.close()

class JsonUtil:
    def __init__(self, file_path: str=''):
        self.extension = '.json'
        self.file_path = file_path

    def save(self, data: List[Dict[str, Any]]):
        origin_data: List[Dict[str, Any]] = []
        
        try:
            with open(self.file_path + self.extension, 'r') as json_file:
                origin_data = json.load(json_file)
        except Exception as error:
            pass
        
        with open(self.file_path + '_tmp' + self.extension, 'w', encoding='utf-8') as json_file:
            try:
                origin_data.extend(data)
                json.dump(origin_data, json_file, ensure_ascii=False)
                os.rename(self.file_path + '_tmp' + self.extension, self.file_path + self.extension)
            except Exception as error:
                logger.error(error)

class CsvUtil:
    def __init__(self, file_path: str='', field_names: List[str]=[]):
        self.extension = '.csv'
        self.file_path = file_path

    def save(self, data: List[Dict[str, Any]]):
        origin_data = []
        
        try:
            with open(self.file_path + self.extension, 'r', newline='') as csv_file:
                rows = csv.DictReader(csv_file)
                origin_data = list(rows)
        except Exception as error:
            pass
        
        with open(self.file_path + '_tmp' + self.extension, 'w', newline='') as csv_file:
            try:
                origin_data.extend(data)
                writer = csv.DictWriter(csv_file, fieldnames=self.field_names)
                writer.writeheader()
                writer.writerows(origin_data)
                os.rename(self.file_path + '_tmp' + self.extension, self.file_path + self.extension)
            except Exception as error:
                logger.error(error)

def init_database(
        site_name: str,
        database_type: DataBaseType, 
        path: str='', 
        file_name: str='',
        fields: Union[DeclarativeMeta, List[str], None]=None,
    ) -> Union[DatabaseUtil, JsonUtil, CsvUtil]:

    if not path:
        path = os.getcwd() + '/data'

    file_path = path + '/' + site_name + "_{:%Y-%m-%d_%H-%M-%S}".format(datetime.now())
    if file_name:
        file_path = path + '/' + file_name

    try:
        if not os.path.exists(path):
            os.makedirs(path)
    except:
        pass

    if database_type is DataBaseType.DATABASE and fields and not isinstance(fields, list):
        database = DatabaseUtil(table=fields, file_path=file_path)
    
    elif database_type is DataBaseType.JSON and not fields:
        database = JsonUtil(file_path=file_path)
    
    elif database_type is DataBaseType.CSV and fields and isinstance(fields, list):
        database = CsvUtil(file_path=file_path, field_names=fields)
    return database