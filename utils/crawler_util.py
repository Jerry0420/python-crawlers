from collections import namedtuple
from enum import Enum
import json
import logging
import os
import multiprocessing
import time
from .logger_util import LoggerToQueue, init_log

class Parser(Enum):
    LXML = 'lxml'
    HTMLPARSER = 'html.parser'
    HTML5LIB = 'html5lib'

Info = namedtuple('Info', ['current_info', 'next_info', 'retry_info'])

class CrawlerUtil:
    
    def __init__(self, database, site_name='') -> None:
        self.collected_data = []
        self.retry_info = []
        self.total_count = 0
        self.database = database
        self.site_name = site_name

        logger_queue = multiprocessing.Queue()
        logger_process = multiprocessing.Process(target=self.init_logger_process, args=(logger_queue,))
        logger_process.start()

        self.logger_process = logger_process
        self.logger = LoggerToQueue(logger_queue)

    def init_logger_process(self, logger_queue):
        logger = init_log(site_name=self.site_name)
        while True:
            while not logger_queue.empty():
                message_info = logger_queue.get()
                log_type = message_info[0]
                log_pid = message_info[1]

                log_filename = message_info[2]
                log_linenum = message_info[3]
                
                log_message = message_info[4]
                log_args = message_info[5]
                log_kargs = message_info[6]

                before_message = f'{log_pid} | {log_filename}:{log_linenum} | '
                
                if log_type == logging.INFO:
                    logger.info(f'{before_message}{log_message}', *log_args, **log_kargs)

    def append(self, data):
        self.collected_data.extend(data)
        if len(self.collected_data) >= 1000:
            self.save()

    def save(self):
        self.total_count += len(self.collected_data)
        self.logger.info("Saved %s into database", len(self.collected_data))
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

    def close(self, session, loop):
        loop.run_until_complete(session.close())
        
        self.logger_process.join(timeout=5)
        self.logger_process.terminate()
        
        loop.stop()
        loop.run_forever()
        loop.close()

    def map(self, pool, function, inputs):
        all_next_info = []
        results_with_all_info = pool.imap_unordered(function, inputs)
        for result_with_all in results_with_all_info:
            if isinstance(result_with_all, tuple):
                result, info = result_with_all
                if info:
                    if info.retry_info:
                        self.retry_info.append(info.retry_info)
                    if info.next_info:
                        all_next_info.append(info.next_info)
            if len(result):
                self.append(result)
        if self.retry_info:
            self.save_retry_info()
        return all_next_info