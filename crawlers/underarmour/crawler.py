import logging
import logging.handlers
import multiprocessing
import os
import sys
from typing import Any, Dict, List, Tuple, Union
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../")

from bs4 import BeautifulSoup
import argparse
import asyncio
from utils.crawler_util import CrawlerUtil, Info, Parser, CrawlerConfig
from utils.database_utils import init_database, DataBaseType
from utils.http_utils import AsyncRequestUtil
from utils.logger_util import MultiProcesses_Logger_Util
from table import Underarmour
from multiprocessing import Pool
from functools import partial
from utils.helper import split_chunk
import json
import math

site_name = 'underarmour'
main_page_url = "https://www.underarmour.tw"

def crawl_page(logger: logging.Logger, document: bytes, url: str, category_url: str):
    results = []
    if not document:
        return results, None
    
    document: BeautifulSoup = BeautifulSoup(document, Parser.LXML.value)

    try:
        items_blocks = document.select('.list-item')
        for item_block in items_blocks:
            item = {}
            price_block = item_block.select_one('.good-price span')
            price = price_block.get_text()
            item['price'] = int(price.replace("NT$", ""))
            a_block = item_block.select_one(".good-txt")
            item['url'] = main_page_url + a_block['href']
            item['title'] = a_block.get_text()
            item['prod_id'] = item['url'].replace("https://www.underarmour.tw/p", '').split("-")[0]
            results.append(item)
    except Exception as error:
        logger.error("Error occurred %s %s", url, category_url)
        return results, Info(next_info=None, retry_info=url)
    logger.info("Crawled %s", url)
    return results, None

def request_page(queue: multiprocessing.Queue, inputs_chunk: List[str]) -> Tuple[List[Dict[str, Any]], List[Info]]:
    sub_logger = logging.getLogger(site_name)
    sub_logger.setLevel(logging.INFO)
    queue_handler = logging.handlers.QueueHandler(queue)

    if len(sub_logger.handlers) == 0 or not isinstance(sub_logger.handlers[0], logging.handlers.QueueHandler):
        sub_logger.addHandler(queue_handler)

    data_of_urls = []
    info_of_urls = []
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    session = AsyncRequestUtil(main_page_url=main_page_url, loop=loop, logger=sub_logger)
    try:
        coroutines = [session.get(param['url'], with_return=param) for param in inputs_chunk]
        coroutines_iterator = asyncio.as_completed(coroutines)
        for coroutine in coroutines_iterator:
            dom, param = loop.run_until_complete(coroutine)
            url = param['url']
            category_url = param['category_url']
            data_per_url, info = crawl_page(sub_logger, dom, url, category_url)
            if data_per_url:
                data_of_urls.extend(data_per_url)
            if info:
                info_of_urls.extend(info)
    except Exception as error:
        sub_logger.error(error)
    finally:
        asyncio.run(session.close())
        return data_of_urls, info_of_urls

def start_crawler(crawler_config: CrawlerConfig):
    logger_queue: multiprocessing.Queue = crawler_config.logger_queue
    crawler_util =crawler_config.crawler_util
    main_logger = logging.getLogger('main')

    pool = Pool(processes=crawler_config.process_num)

    total_urls = []

    with open('categories.json', 'r') as openfile: 
        categories = json.load(openfile) 

    for category in categories:
        nav = category['nav']
        category_url = category['url']
        total_page = math.ceil(category['total'] / 40) # 40 items in one page.
        for page in range(1, total_page + 1):
            url = f'{main_page_url}/sys/navigation/loading?nav={nav}&pageNumber={page}'
            total_urls.append({"url": url, "category_url": category_url})

    inputs_chunks = split_chunk(total_urls, crawler_config.chunk_size)
    
    try:
        _ = crawler_util.imap(pool, partial(request_page, logger_queue), inputs_chunks)
    except Exception as error:
        main_logger.error(error)
    finally:
        crawler_util.save()
        main_logger.info('Total saved %s into database.', crawler_util.total_count)

        logger_util.close()
        crawler_util.close(pool=pool)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--processes", help="crawl with n processes", type=int, default=5)
    parser.add_argument("-c", "--chunk_size", help="size of tasks inside one process.", type=int, default=20)
    args = parser.parse_args()

    logger_util = MultiProcesses_Logger_Util(site_name)
    database = init_database(database_type=DataBaseType.DATABASE, site_name=site_name, fields=Underarmour)
    crawler_util = CrawlerUtil(database=database)

    crawler_config = CrawlerConfig(crawler_util=crawler_util, logger_queue=logger_util.queue, process_num=args.processes, chunk_size=args.chunk_size)    
    start_crawler(crawler_config)