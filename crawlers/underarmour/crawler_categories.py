import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../")

from bs4 import BeautifulSoup
import asyncio
from utils.crawler_util import CrawlerUtil, Info, Parser
from utils.database_utils import init_database, DataBaseType
from utils.http_utils import AsyncRequestUtil
from utils.logger_util import LoggerUtil, LogToQueue
from multiprocessing import Pool
import re
from functools import partial

site_name = 'underarmour'
main_page_url = "https://www.underarmour.tw"
logger_util = LoggerUtil(site_name=site_name)
database = init_database(database_type=DataBaseType.JSON, logger_util=logger_util, site_name=site_name, path=os.getcwd(), file_name='categories')
crawler_util = CrawlerUtil(database=database, logger_util=logger_util)

def crawl_page(logger: LogToQueue, document: bytes):
    document: BeautifulSoup = BeautifulSoup(document, Parser.LXML.value)
    results = []
    if not document:
        return []
    
    try:
        men_blocks = document.select_one('.nav-li-men')
        women_blocks = document.select_one('.nav-li-women')
        junior_blocks = document.select_one('.nav-li-junior')
        
        men_blocks.append(women_blocks)
        men_blocks.append(junior_blocks)

        men_women_junior_blocks = men_blocks

        urls = men_women_junior_blocks.select('.menu-li-common > a:first-of-type')
        for url in urls:
            url = url.get('href')
            if 'cmens-' in url or 'cwomens-' in url or 'cyouth_' in url:
                url = main_page_url + url
                results.append(url)
    except Exception as error:
        logger.error("Error occurred.")
        return []
    return results

def request_categories(logger: LogToQueue, url: str):
    data_of_urls = []
    loop = asyncio.get_event_loop()
    session = AsyncRequestUtil(loop=loop, logger=logger)

    try:
        document = loop.run_until_complete(session.get(url))
        data_of_urls = crawl_page(logger, document)
    except Exception as error:
        logger_util.logger.error(error)
    finally:
        asyncio.run(session.close())
        return data_of_urls

def start_crawler():
    logger_util.init_logger_process_and_logger()

    try:
        data_of_urls = request_categories(logger_util.logger, main_page_url)
        crawler_util.extend(data_of_urls)
    except Exception as error:
        logger_util.logger.error(error)
    finally:
        crawler_util.save()
        logger_util.logger.info('Total saved %s categories.', crawler_util.total_count)
        logger_util.close()

if __name__ == "__main__":
    start_crawler()