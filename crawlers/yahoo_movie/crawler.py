import os
import sys
import traceback
from typing import Any, Dict, List, Tuple, Union
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../")

from bs4 import BeautifulSoup
import argparse
import asyncio
from utils.crawler_util import CrawlerUtil, Info, Parser, CrawlerConfig
from utils.database_utils import init_database, DataBaseType
from utils.http_utils import AsyncRequestUtil
from utils.logger_util import LoggerUtil, LogToQueue
from table import YahooMovie
from multiprocessing import Pool
from datetime import datetime
import re
from functools import partial
from utils.helper import split_chunk

site_name = 'yahoo_movie'
main_page_url = "https://movies.yahoo.com.tw/index.html"

def crawl_page(logger: LogToQueue, document: bytes):
    result = {}
    if not document:
        return [], None

    document = BeautifulSoup(document, Parser.LXML.value)
    
    url_block = document.select_one('meta[property="og:url"]')
    if not url_block or url_block and  '/id=' not in url_block['content']: 
        return [], None

    movie_info_block = document.select_one('.movie_intro_info_r')
    name_ch_block = movie_info_block.select_one('h1') if movie_info_block else ''
    name_en_block = movie_info_block.select_one('h3') if movie_info_block else ''
    genres_blocks = movie_info_block.select('.level_name')
    all_info = movie_info_block.select('span')
    big_image_block = document.select_one('.movie_intro_info_l .btn_zoomin')
    image_block = document.select_one('meta[property="og:image"]')
    content_block = document.select_one('#story')
    yahoo_score_block = document.select_one('.score_num.count')
    vote_count_block = document.select_one('.starbox2 span')

    try:
        result['url'] = url_block['content']
        result['movie_id'] = int(result['url'].split('=')[-1])
        result['name_ch'] = name_ch_block.text if name_ch_block else ''
        result['name_en'] = name_en_block.text if name_en_block else ''
        
        genres = ""
        for genres_block in genres_blocks:
            genres += genres_block.text.strip() + '|'
        result['genres'] = genres[:-1] if genres else ''

        release_date = None #date
        company = ''
        imdb_score = 0.0
        directors = ''
        actors = ''
        for info in all_info:
            if '上映日期' in info.text:
                release_date = info.text.split('：')[-1]
            if '發行公司' in info.text:
                company = info.text.split('：')[-1]
            if 'IMDb分數' in info.text:
                imdb_score = info.text.split('：')[-1]
            if '導演' in info.text:
                directors = info.findNext('div').text.strip().replace(' ', '').replace('\n', '').replace('、', '|')
            if '演員' in info.text:
                actors = info.findNext('div').text.strip().replace(' ', '').replace('\n', '').replace('、', '|')
        result['release_date'] = datetime.strptime(release_date, "%Y-%m-%d").date() if release_date and '未定' not in release_date else None
        result['company'] = company
        result['imdb_score'] = float(imdb_score)
        result['directors'] = directors
        result['actors'] = actors

        img_url = ''
        if big_image_block:
            img_url = big_image_block['href']
        elif image_block:
            img_url = image_block['content']
        result['img_url'] = img_url

        result['content'] = content_block.text.strip().replace('\r', '').replace('\n', '') if content_block else ''
        # 滿分 5
        result['yahoo_score'] = float(yahoo_score_block.text) if yahoo_score_block else 0.0
        
        vote_count = vote_count_block.text if vote_count_block else ''
        vote_count = re.findall(r'\d+', vote_count)
        result['vote_count'] = int(vote_count[0]) if vote_count else 0
    except Exception as error:
        logger.error("Error occurred %s ", result['url'])
        return [], Info(next_info=None, retry_info=result['url'])
    logger.info("Crawled %s", result['url'])
    return [result], None

def retry_function(status_code: int, response: Union[Dict[str, Any], bytes, None], **kwargs) -> bool:
    result = False
    if status_code in [200, 204] and response:
        result = True
    if status_code in [302, 500]:
        result = True
    return result

def request_page(logger: LogToQueue, inputs_chunk: List[str]) -> Tuple[List[Dict[str, Any]], List[Info]]:
    data_of_urls = []
    info_of_urls = []
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    session = AsyncRequestUtil(main_page_url=main_page_url, loop=loop, logger=logger)
    try:
        coroutines = [session.get(url, allow_redirects=False, retry_function=retry_function) for url in inputs_chunk]
        coroutines_iterator = asyncio.as_completed(coroutines)
        for coroutine in coroutines_iterator:
            dom = loop.run_until_complete(coroutine)
            data_per_url, info = crawl_page(logger, dom)
            if data_per_url:
                data_of_urls.extend(data_per_url)
            if info:
                info_of_urls.extend(info)
    except Exception as error:
        logger.error(error)
    finally:
        asyncio.run(session.close())
        return data_of_urls, info_of_urls

def start_crawler(crawler_config: CrawlerConfig, upper_limit):
    logger_util = crawler_config.logger_util
    crawler_util =crawler_config.crawler_util

    # must init all processes inside main function.
    pool = Pool(processes=crawler_config.process_num)
    logger_util.init_logger_process_and_logger()

    inputs_chunks = split_chunk(
        [f"https://movies.yahoo.com.tw/movieinfo_main.html/id={i}" for i in range(1, upper_limit)], 
        crawler_config.chunk_size
    )
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
    parser.add_argument("-u", "--upper_limit", help="upper limit of this website.", type=int, default=300) # 12900
    args = parser.parse_args()

    logger_util = LoggerUtil(site_name=site_name)
    database = init_database(database_type=DataBaseType.DATABASE, site_name=site_name, fields=YahooMovie, logger_util=logger_util)
    crawler_util = CrawlerUtil(database=database, logger_util=logger_util)
    crawler_config = CrawlerConfig(crawler_util=crawler_util, logger_util=logger_util, process_num=args.processes, chunk_size=args.chunk_size)
    start_crawler(crawler_config, args.upper_limit)