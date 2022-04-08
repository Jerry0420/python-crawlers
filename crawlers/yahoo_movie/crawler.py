import os
import sys
import traceback
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../")

from bs4 import BeautifulSoup
import argparse
import asyncio
from utils.crawler_util import CrawlerUtil, Info
from utils.database_utils import init_database, DataBaseType
from utils.http_utils import AsyncRequestUtil
from utils.logger_util import LoggerToQueue
from table import YahooMovie
from multiprocessing import Pool
from datetime import datetime
import re
from functools import partial

site_name = 'yahoo_movie'
main_page_url = "https://movies.yahoo.com.tw/index.html"
database = init_database(database_type=DataBaseType.DATABASE, file_name=site_name, fields=YahooMovie)
session = AsyncRequestUtil(main_page_url=main_page_url)
loop = asyncio.get_event_loop()
crawler_util = CrawlerUtil(database=database, site_name=site_name)

def split_chunk(list, n=100):
    for i in range(0, len(list), n):
        yield list[i: i + n]

def get_page(logger: LoggerToQueue, document):
    result = {}
    result['id'] = document['args']['id']
    logger.info("Crawled %s", result['id'])
    return [result], None

async def request_page(url):
    response = await session.get(url=url, json_response=True)
    return response

def start_crawler(process_num, upper_limit, chunk_size):
    pool = Pool(processes=process_num)
    inputs_chunks = split_chunk(
        [request_page(f"https://httpbin.org/get?id={i}".format(i)) for i in range(1, upper_limit)], 
        chunk_size
    )
    try:
        for inputs_chunk in inputs_chunks:
            try:
                done_response, pending = loop.run_until_complete(asyncio.wait(inputs_chunk))
                all_page_dom = [response.result() for response in done_response]
                _ = crawler_util.map(pool, partial(get_page, crawler_util.__class__.logger), all_page_dom)
            except Exception as error:
                crawler_util.__class__.logger.error(error)
    except KeyboardInterrupt as error:
        pass
    finally:
        crawler_util.save()
        crawler_util.close(session=session, loop=loop)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--processes", help="crawl with n processes", type=int, default=8)
    args = parser.parse_args()

    start_crawler(args.processes, 10, 1)