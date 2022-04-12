import os
import sys
import traceback
from typing import Any, Dict, List, Tuple, Union
from unittest import result
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../")

from bs4 import BeautifulSoup
import argparse
import asyncio
from utils.crawler_util import CrawlerUtil, Info, Parser
from utils.database_utils import init_database, DataBaseType
from utils.http_utils import AsyncRequestUtil
from utils.logger_util import LoggerUtil, LogToQueue
from table import Underarmour
from multiprocessing import Pool
from datetime import datetime
import re
from functools import partial
from utils.helper import split_chunk
import json
import math

site_name = 'underarmour'
main_page_url = "https://www.underarmour.tw"
logger_util = LoggerUtil(site_name=site_name)
database = init_database(database_type=DataBaseType.DATABASE, site_name=site_name, fields=Underarmour, logger_util=logger_util)
crawler_util = CrawlerUtil(database=database, logger_util=logger_util)

def crawl_page(logger: LogToQueue, document: bytes, url: str, category_url: str):
    document = BeautifulSoup(document, Parser.LXML.value)
    results = []

    if not document:
        return results, None

    try:
        print(document)
        result = {}
        results.append(result)
    except Exception as error:
        logger.error("Error occurred %s %s", url, category_url)
        return results, Info(next_info=None, retry_info=url)
    logger.info("Crawled %s", url)
    return results, None

def request_page(logger: LogToQueue, inputs_chunk: List[str]) -> Tuple[List[Dict[str, Any]], List[Info]]:
    data_of_urls = []
    info_of_urls = []
    loop = asyncio.new_event_loop()
    session = AsyncRequestUtil(main_page_url=main_page_url, loop=loop, logger=logger)
    try:
        for task in inputs_chunk:
            url = task
            category_url = task
            dom = loop.run_until_complete(session.get(url))
            data_per_url, info = crawl_page(logger, dom, url, category_url)
            if data_per_url:
                data_of_urls.extend(data_per_url)
            if info:
                info_of_urls.extend(info)
    except Exception as error:
        logger.error(error)
    finally:
        asyncio.run(session.close())
        return data_of_urls, info_of_urls

def start_crawler(process_num, chunk_size):
    # must init all processes inside main function.
    pool = Pool(processes=process_num)
    logger_util.init_logger_process_and_logger()

    total_urls = []

    with open('categories.json', 'r') as openfile: 
        categories = json.load(openfile) 

    for category in categories:
        nav = category['nav']
        category_url = category['url']
        total_page = math.ceil(category['total'] / 40) # 40 items in a page.
        for page in range(1, total_page + 1):
            url = f'{main_page_url}/sys/navigation/loading?nav={nav}&pageNumber={page}'
            total_urls.append({"url": url, "category_url": category_url})

    inputs_chunks = split_chunk(total_urls, chunk_size)
    
    inputs_chunks = [['https://www.underarmour.tw/sys/navigation/loading?nav=3&pageNumber=1']]
    
    try:
        _ = crawler_util.imap(pool, partial(request_page, logger_util.logger), inputs_chunks)
    except Exception as error:
        logger_util.logger.error(error)
    finally:
        crawler_util.save()
        logger_util.logger.info('Total saved %s into database.', crawler_util.total_count)

        logger_util.close()
        crawler_util.close(pool=pool)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--processes", help="crawl with n processes", type=int, default=5)
    parser.add_argument("-c", "--chunk_size", help="size of tasks inside one process.", type=int, default=20)
    args = parser.parse_args()
    start_crawler(args.processes, args.chunk_size)