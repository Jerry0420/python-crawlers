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

from utils.logger_util import LogToQueue

class DataBaseType(Enum):
    DATABASE = 'DATABASE' # init_database(database_type=DataBaseType.DATABASE, file_name=site_name, fields=GlobalWifi)
    JSON = 'JSON' #  init_database(database_type=DataBaseType.CSV, file_name=site_name, fields=['title', 'id'])
    CSV = 'CSV' # init_database(database_type=DataBaseType.JSON, file_name=site_name, fields=None)

class DatabaseUtil:
    def __init__(self, table: DeclarativeMeta, path: str='', file_name: str='', logger: Optional[LogToQueue]=None):
        self.table = table
        self.path = path + '/data'
        self.file_name = file_name + "_{:%Y-%m-%d_%H-%M-%S}".format(datetime.now()) + ".sqlite3"
        self.logger = logger

        try:
            if not os.path.exists(self.path):
                os.makedirs(self.path)
            Path(self.path + '/' + self.file_name).touch()
            self.engine: Engine = create_engine('sqlite:///{}'.format(self.path + '/' + self.file_name))
            self.table.metadata.create_all(self.engine)
        except Exception as error:
            self.logger.critical(error)
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
            self.logger.error(error)
        finally:
            session.close()

class JsonUtil:
    def __init__(self, path: str='', file_name: str='', logger: Optional[LogToQueue]=None):
        self.path = path + '/data'
        self.file_name = file_name + "_{:%Y-%m-%d_%H-%M-%S}".format(datetime.now())
        self.logger = logger

        try:
            if not os.path.exists(self.path):
                os.makedirs(self.path)
        except Exception as error:
            logger.critical(error)
            exit()

    def save(self, data: List[Dict[str, Any]]):
        origin_data: List[Dict[str, Any]] = []
        
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
                self.logger.error(error)

class CsvUtil:
    def __init__(self, path: str='', file_name: str='', field_names: List[str]=[], logger: Optional[LogToQueue]=None):
        self.path = path + '/data'
        self.file_name = file_name + "_{:%Y-%m-%d_%H-%M-%S}".format(datetime.now())
        self.field_names = field_names
        self.logger = logger

        try:
            if not os.path.exists(self.path):
                os.makedirs(self.path)
        except Exception as error:
            logger.critical(error)
            exit()

    def save(self, data: List[Dict[str, Any]]):
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
                self.logger.error(error)

def init_database(
        database_type: DataBaseType=DataBaseType.DATABASE, 
        file_name: str='',
        fields: Union[DeclarativeMeta, List[str], None]=None,
        path: str=os.getcwd(), 
        logger: Optional[LogToQueue]=None
    ) -> Union[DatabaseUtil, JsonUtil, CsvUtil]:
    if database_type is DataBaseType.DATABASE and fields and not isinstance(fields, list):
        database = DatabaseUtil(table=fields, path=path, file_name=file_name, logger=logger)
    
    elif database_type is DataBaseType.JSON and not fields:
        database = JsonUtil(path=path, file_name=file_name, logger=logger)
    
    elif database_type is DataBaseType.CSV and fields and isinstance(fields, list):
        database = CsvUtil(path=path, file_name=file_name, field_names=fields, logger=logger)
    return database