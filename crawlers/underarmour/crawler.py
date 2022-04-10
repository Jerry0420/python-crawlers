import os
import sys
import traceback
from typing import Any, Dict, List, Tuple, Union
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../")

from bs4 import BeautifulSoup
import argparse
import asyncio
from utils.crawler_util import CrawlerUtil, Info
from utils.database_utils import init_database, DataBaseType
from utils.http_utils import AsyncRequestUtil
from utils.logger_util import LoggerUtil, LogToQueue
from table import Underarmour
from multiprocessing import Pool
from datetime import datetime
import re
from functools import partial
from utils.helper import split_chunk
import time

site_name = 'underarmour'
main_page_url = "https://www.underarmour.tw"
logger_util = LoggerUtil(site_name=site_name)
database = init_database(database_type=DataBaseType.DATABASE, site_name=site_name, fields=Underarmour, logger_util=logger_util)
crawler_util = CrawlerUtil(database=database, logger_util=logger_util)

def crawl_categories():
    json_database = init_database(database_type=DataBaseType.JSON, logger_util=logger_util, site_name=site_name, path=os.getcwd(), file_name='categories')
    crawler_util.__class__.database = json_database
    loop = asyncio.new_event_loop()
    session = AsyncRequestUtil(main_page_url=main_page_url, loop=loop, logger=logger_util.logger)

    done = False

    try:
        asyncio.run(session.close())
        done = True
    except Exception as error:
        logger_util.logger.error(error)
    finally:
        crawler_util.save()
        crawler_util.__class__.database = database
        crawler_util.reset()
        return done

def start_crawler(process_num, chunk_size):
    logger_util.init_logger_process_and_logger()

    result_of_crawl_categories = crawl_categories()
    
    pool = Pool(processes=process_num)


    try:
        pass
    except Exception as error:
        pass
    finally:
        logger_util.close()
        crawler_util.close(pool=pool)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--processes", help="crawl with n processes", type=int, default=5)
    parser.add_argument("-c", "--chunk_size", help="size of tasks inside one process.", type=int, default=20)
    args = parser.parse_args()
    start_crawler(args.processes, args.chunk_size)