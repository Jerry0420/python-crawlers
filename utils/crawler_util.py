from collections import namedtuple
from enum import Enum
import json
import logging
import multiprocessing
import os

from .database_utils import DatabaseUtil, JsonUtil, CsvUtil
from typing import Optional, Union, List, Dict, Any, Tuple, Iterator, Callable
from multiprocessing.pool import Pool

class Parser(Enum):
    LXML = 'lxml'
    HTMLPARSER = 'html.parser'
    HTML5LIB = 'html5lib'

Info = namedtuple('Info', ['next_info', 'retry_info'])
logger = logging.getLogger('crawler_util')

class CrawlerUtil:

    database: Union[DatabaseUtil, JsonUtil, CsvUtil, None] = None
    
    def __init__(self, database: Union[DatabaseUtil, JsonUtil, CsvUtil]) -> None:
        self.collected_data = []
        self.retry_info = []
        self.total_count = 0
        self.__class__.database = database

    def set_database(self, database: Union[DatabaseUtil, JsonUtil, CsvUtil]):
        self.__class__.database = database

    def extend(self, data: List[Dict[str, Any]]):
        self.collected_data.extend(data)
        if len(self.collected_data) >= 500:
            self.save()

    def save(self):
        self.total_count += len(self.collected_data)
        logger.info("Saved %s into database", len(self.collected_data))
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

    def reset(self):
        self.collected_data = []
        self.retry_info = []
        self.total_count = 0

    def close(self, pool: Pool):
        pool.terminate()

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

class CrawlerConfig:
    def __init__(self, crawler_util: Optional[CrawlerUtil]=None, logger_queue: Optional[multiprocessing.Queue] = None, process_num: Optional[int]=None, chunk_size: Optional[int]=None) -> None:
        self.crawler_util = crawler_util
        self.process_num = process_num
        self.chunk_size = chunk_size
        self.logger_queue = logger_queue