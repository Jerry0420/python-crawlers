from collections import namedtuple
from enum import Enum
import json
import os
import time

from utils.logger_util import LoggerUtil
from .database_utils import DatabaseUtil, JsonUtil, CsvUtil
from typing import Optional, Union, List, Dict, Any, Tuple, Iterator, Callable
from multiprocessing.pool import Pool

class Parser(Enum):
    LXML = 'lxml'
    HTMLPARSER = 'html.parser'
    HTML5LIB = 'html5lib'

Info = namedtuple('Info', ['current_info', 'next_info', 'retry_info'])

class CrawlerUtil:

    database: Union[DatabaseUtil, JsonUtil, CsvUtil, None] = None
    logger_util: Optional[LoggerUtil] = None
    
    def __init__(self, database: Union[DatabaseUtil, JsonUtil, CsvUtil], logger_util: LoggerUtil) -> None:
        self.collected_data = []
        self.retry_info = []
        self.total_count = 0
        self.__class__.database = database
        self.__class__.logger_util = logger_util

    def extend(self, data: List[Dict[str, Any]]):
        self.collected_data.extend(data)
        if len(self.collected_data) >= 500:
            self.save()

    def save(self):
        self.total_count += len(self.collected_data)
        self.__class__.logger_util.logger.info("Saved %s into database", len(self.collected_data))
        self.database.save(self.collected_data)
        self.collected_data = []

    def save_retry_info(self):
        retry_info_file_path = 'retry_info.json'
        previous_retry_info = []
        if os.path.isfile(retry_info_file_path):
            with open(retry_info_file_path, 'r') as openfile: 
                previous_retry_info = json.load(openfile)
        previous_retry_info.extend(self.retry_info)
        with open(retry_info_file_path, 'w', encoding='utf-8') as f:
            json.dump(previous_retry_info, f, ensure_ascii=False)
        self.retry_info = []

    def close(self):
        if self.__class__.logger_util and self.__class__.logger_util.logger_process:
            time.sleep(3)
            self.__class__.logger_util.close()

    def imap(self, pool: Pool, function: Callable[[Any], Any], inputs: List[Any]) -> List[Any]:
        all_next_info = []
        results: Iterator[Union[Tuple[List[Any], List[Info]], List[Any]]] = \
            pool.imap_unordered(function, inputs, )
        for result in results:
            collected_data = []
            if isinstance(result, tuple):
                data_of_urls, info_of_urls = result
                if info_of_urls:
                    for info in info_of_urls:
                        if info.retry_info:
                            self.retry_info.append(info.retry_info)
                        if info.next_info:
                            all_next_info.append(info.next_info)
                collected_data = data_of_urls
            else:
                collected_data = result
            if len(collected_data):
                self.extend(collected_data)
        if self.retry_info:
            self.save_retry_info()
        return all_next_info