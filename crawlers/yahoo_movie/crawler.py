import os
import sys
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
import multiprocessing
from datetime import datetime
import re
from functools import partial
from utils.helper import split_chunk

site_name = 'yahoo_movie'
main_page_url = "https://movies.yahoo.com.tw/index.html"
database = init_database(database_type=DataBaseType.DATABASE, file_name=site_name, fields=YahooMovie)
session = AsyncRequestUtil(main_page_url=main_page_url)
loop = asyncio.get_event_loop()
crawler_util = CrawlerUtil(database=database, site_name=site_name)

def get_page(logger: LoggerToQueue, document):
    document = BeautifulSoup(document, 'lxml')
    result = {}
    url_block = document.select_one('meta[property="og:url"]')
    if url_block and  '/id=' not in url_block['content']: return []

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
        return [], Info(current_info=None, next_info=None, retry_info=result['url'])
    logger.info("Crawled %s", result['url'])
    return [result]

async def request_page(url):
    response = await session.get(url=url)
    return response

def start_crawler(process_num, upper_limit, chunk_size):
    pool = Pool(processes=process_num)
    inputs_chunks = split_chunk(
        [request_page(f"https://movies.yahoo.com.tw/movieinfo_main.html/id={i}".format(i)) for i in range(1, upper_limit)], 
        chunk_size
    )
    try:
        for inputs_chunk in inputs_chunks:
            try:
                done_response, pending = loop.run_until_complete(asyncio.wait(inputs_chunk))
                all_page_dom = [response.result() for response in done_response]
                _ = crawler_util.map(pool, partial(get_page, crawler_util.logger), all_page_dom)
            except Exception as error:
                crawler_util.logger.error(error)
    except KeyboardInterrupt as error:
        pass
    finally:
        crawler_util.save()
        crawler_util.close(session=session, loop=loop)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--processes", help="crawl with n processes", type=int, default=(multiprocessing.cpu_count() - 1))
    args = parser.parse_args()

    start_crawler(args.processes, 12900, 100)