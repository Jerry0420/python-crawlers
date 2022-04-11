# Python Crawlers

# Table Of Contents
- [Python Crawlers](#python-crawlers)
- [Table Of Contents](#table-of-contents)
- [Overview](#overview)
- [Key Features](#key-features)
- [Utilities & Concepts](#utilities--concepts)
    - [HTTP Utility](#http-utility)
    - [Database Utility](#database-utility)
    - [Logging Utility](#logging-utility)
    - [Crawler Utility](#crawler-utility)
- [Crawlers Examples](#crawlers-examples)
- [TODO](#todo)

# Overview
This project is a toolkit that wraps all Python utilities for web crawling. It includes a networking utility, logging utility, concurrency programming utility, and database utility.

# Key Features
There are two stages in the run time of a web crawler program.
1. I/O-intensive stage of networking.
2. CPU-intensive stage of crawling HTTP responses.

A web crawler that utilizes this project will use the Python multiprocessing module to distribute multiple URLs into multiple processes. Then, it creates many coroutines in each process to execute I/O-intensive networking tasks asynchronously. Once any HTTP response return, the process will crawl the response, and it is a CPU-intensive task. A web crawler will continue these two stages back and forth until all URLs are scraped.

For example, five hundred URLs need to be scraped and my computer can create five processes to handle these tasks simultaneously. 

First, the web crawler will split these five hundred URLs into twenty-five chunks (twenty URLs per chunk). 

Second, one chunk will be assigned to a process, which means there are five chunks to be handled inside five processes, which also means there are one hundred HTTP requests (5 * 20) to be sent simultaneously. Once any HTTP response return, the process will crawl the response. 

Finally, when tasks (twenty URLs) of one chunk are all done in one process, the web crawler will assign another chunk to this process.

Because of asynchronous execution, one process can wait for multiple HTTP requests to return simultaneously. Besides, due to the multi-core computer structure, multiple processes can crawl multiple responses parallelly when any response return.   

Please note the following two points:
1. The number of processes must coordinate with the number of computer cores. More processes don't mean a faster performance of the program. If there are too many processes running inside one program, it will cause more context switches between processes and lower the speed of that program.
2. Consider the network speed, don't assign too many tasks inside one process at a time. Because of asynchronous execution, one process can perform multiple HTTP requests simultaneously and multiple processes are running inside a web crawler. Therefore, it will make many HTTP requests wait together and lead to timeout problems.

# Utilities & Concepts
### [HTTP Utility](https://github.com/Jerry0420/python-crawlers/blob/main/utils/http_utils.py) 
* Developed by the Python [AIOHTTP](https://github.com/aio-libs/aiohttp) module.
* The following points are the main reasons that cause networking tasks to fail when web crawling:
  * Too many requests cause blocking of the IP address or the user-agent.
  * Too many requests trigger the defense mechanism of the website for not processing any request.
  * Incorrect cookies inside the request.
* The HTTP Utility wraps the following methods to solve these problems:
  * HTTP request retry.
  * Initial cookies.
  * User-agent rotation.
  * Proxies rotation.

### [Database Utility](https://github.com/Jerry0420/python-crawlers/blob/main/utils/database_utils.py) 
* There are three storage modes for the Database Utility:
   * SQLite
   * CSV
   * JSON
* The Database Utility defines the same interface for three storage modes. We can directly store data in the type of Python dictionary list. 
* SQLite
    * Developed by the Python [SQLAlchemy](https://github.com/sqlalchemy/sqlalchemy) module.
* CSV
    * Developed by the Python csv module.

### [Logging Utility](https://github.com/Jerry0420/python-crawlers/blob/main/utils/logger_util.py)
* A web crawler that utilizes this web crawler toolkit is under a multi-process environment. It will cause race condition problems if multiple processes write log messages into a log file together. The following points will solve this problem:
  * The web crawler will create a process and a multiprocessing queue. Inside the process, the web crawler receives all log messages from the multiprocessing queue and writes them into a log file.
  * Pass this multiprocessing queue to every process to collect all log messages.

### [Crawler Utility](https://github.com/Jerry0420/python-crawlers/blob/main/utils/crawler_util.py)
* The Crawler Utility wraps APIs of the multiprocessing module and the Database Utility, we can just pass a multiprocessing pool and a crawler function into the API and is good to go.
* The Crawler Utility will temporarily save all collected data into the memory. Once the web crawler collects more than five hundred records, the Crawler Utility will use the Database Utility to move all data into the database. ( or the CSV/ JSON file)
* The Crawler Utility will save all failure URLs into a retry_info.json file for recrawling again in the future.

# Crawlers Examples
<table>
  <tr>
    <th>Site Name</th>
    <th>Site URL</th>
    <th>Code</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>Yahoo Movie</td>
    <td><a href="https://movies.yahoo.com.tw/index.html">Link</a></td>
    <td><a href="https://github.com/Jerry0420/python-crawlers/tree/main/crawlers/yahoo_movie">Link</a></td>
    <td>Crawl all movies.</td>
  </tr>
  <tr>
    <td>Under Armour</td>
    <td><a href="https://www.underarmour.tw">Link</a></td>
    <td><a href="https://github.com/Jerry0420/python-crawlers/tree/main/crawlers/underarmour">Link</a></td>
    <td>Crawl all products.</td>
  </tr>
</table>

# TODO
1. add selenum support
2. add proxies crawler
3. add user agents
