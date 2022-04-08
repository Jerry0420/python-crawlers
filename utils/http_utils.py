import aiohttp
import asyncio
from enum import Enum
from .user_agents import OS, Browser, get_user_agent, UserAgentType
from .proxies import get_proxy
import logging
from typing import Any, Union, List, Dict, Callable, Optional

logger = logging.getLogger(__name__)

class HTTPMethods(Enum):
    GET = "GET"
    POST = "POST"

boundary = "----WebKitFormBoundaryJXrxC79Dpyal5JFf"

class ContentType(Enum):
    TEXTHTML = "text/html; charset=UTF-8"
    JSON = "application/json"
    URLENCODED = "application/x-www-form-urlencoded"
    MULTIPART = "multipart/form-data" + '; boundary={}'.format(boundary)

default_headers = {
    "Accept": 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-TW,zh;q=0.8,en-US;q=0.5,en;q=0.3",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
}

class AsyncRequestUtil:
    
    def __init__(
            self, 
            main_page_url: str=None, 
            retry_times: int=5, 
            sleep_seconds: int=30, 
            default_headers_enable: bool=True, 
            headers: Union[str, Dict[str, str]]={}, 
            cookies: Union[str, Dict[str, str]]={}, 
            timeout: int=30, 
            proxy_countries: List[str]=None, 
            user_agent_type: UserAgentType=UserAgentType(OS.MACOS, Browser.CHROME)
        ):
        
        self.session = aiohttp.ClientSession()
        self.main_page_url = main_page_url
        self.retry_times = retry_times
        self.timeout = timeout
        self.sleep_seconds = sleep_seconds
        self.user_agent_type = user_agent_type
        self.headers = default_headers if default_headers_enable else {}
        self.headers['user-agent'] = get_user_agent(user_agent_type)
        
        if isinstance(headers, str):
            headers = self.__class__.__get_headers_dict_from_string(headers)
        self.headers.update(headers)

        if isinstance(cookies, str):
            cookies = self.__class__.__get_cookies_dict_from_string(cookies)
        self.cookies = cookies

        self.proxy = None
        self.proxy_countries = proxy_countries
        if proxy_countries:
            self.proxy = get_proxy(proxy_countries)

    @classmethod
    def __get_cookies_dict_from_string(cls, cookies_string: str) -> Dict[str, str]:
        cookies = {}
        for line in cookies_string.split(';'):
            key, value = line.strip().split('=', 1)
            cookies[key]=value
        return cookies

    @classmethod
    def __get_headers_dict_from_string(cls, headers_string: str) -> Dict[str, str]:
        headers = {}
        for line in headers_string.split('\n'):
            splitted_header = line.strip().split(':')
            if len(splitted_header) == 2:
                headers[splitted_header[0]] = splitted_header[1]
        return headers

    async def reset(self):
        await asyncio.sleep(self.sleep_seconds)
        self.headers['user-agent'] = get_user_agent(self.user_agent_type)
        if self.proxy_countries:
            self.proxy = get_proxy(self.proxy_countries)
        await self.init_cookie()

    async def close(self):
        await self.session.close()

    async def init_cookie(self):
        if self.main_page_url:
            response = await self.session.get(self.main_page_url, ssl=False)
            cookies = response.cookies
            cookies.update(self.cookies)
            self.cookies = cookies

    async def get(
            self, 
            url: str, 
            query_strings: Optional[Dict[str, str]]=None, 
            headers: Union[str, Dict[str, str], None]=None, 
            cookies: Union[str, Dict[str, str], None]=None, 
            referer: Optional[str]=None, 
            allow_redirects: bool=True, 
            json_response: bool=False, 
            retry_function: Optional[Callable[[Any], bool]]=None
        ) -> Union[None, Dict[str, Any], bytes]:
        response = await self.__request(
                self.session.get, 
                url, 
                query_strings, 
                body=None, 
                json_body=None, 
                headers=headers, 
                cookies=cookies, 
                referer=referer, 
                allow_redirects=allow_redirects, 
                json_response=json_response, 
                retry_function=retry_function
            )
        return response

    async def post(
            self, 
            url: str, 
            query_strings: Optional[Dict[str, str]]=None, 
            body: Union[Dict[str, Any], bytes, None]=None, 
            json_body: Optional[Dict[str, Any]]=None, 
            headers: Union[str, Dict[str, str], None]=None, 
            cookies: Union[str, Dict[str, str], None]=None, 
            referer: Optional[str]=None, 
            allow_redirects: bool=True, 
            json_response: bool=False,
            retry_function: Optional[Callable[[Any], bool]]=None
        ) -> Union[None, Dict[str, Any], bytes]:
        response = await self.__request(
                self.session.post, 
                url, 
                query_strings, 
                body=body, 
                json_body=json_body, 
                headers=headers, 
                cookies=cookies, 
                referer=referer, 
                allow_redirects=allow_redirects, 
                json_response=json_response, 
                retry_function=retry_function
            )
        return response

    def __retry_function(self, status_code: int, response: Union[Dict[str, Any], bytes, None]) -> bool:
        result = False
        try:
            if status_code in [200, 204] and response:
                result = True
        except Exception as error:
            logger.warning('Retry %s', response.url)
        return result

    async def __request(
            self, 
            method, 
            url: str,
            query_strings: Optional[Dict[str, str]]=None, 
            body: Union[Dict[str, Any], bytes, None]=None, 
            json_body: Optional[Dict[str, Any]]=None, 
            headers: Union[str, Dict[str, str], None]=None, 
            cookies: Union[str, Dict[str, str], None]=None, 
            referer: Optional[str]=None, 
            allow_redirects: bool=True, 
            json_response: bool=False,
            retry_function: Optional[Callable[[Any], bool]]=None
        ) -> Union[None, Dict[str, Any], bytes]:

        if referer:
            self.headers['referer'] = referer
        
        if headers:
            if isinstance(headers, str):
                headers = self.__class__.__get_headers_dict_from_string(headers)
            self.headers.update(headers)

        if cookies:
            if isinstance(cookies, str):
                cookies = self.__class__.__get_cookies_dict_from_string(cookies)
            self.cookies.update(cookies)

        retry_function = retry_function if retry_function else self.__retry_function
        for _ in range(self.retry_times):
            try:
                response = await method(
                    url=url,
                    params=query_strings,
                    headers=self.headers,
                    json=json_body,
                    data=body,
                    cookies=self.cookies,
                    timeout=self.timeout,
                    allow_redirects=allow_redirects,
                    proxy=self.proxy,
                    ssl=False
                )
                
                if json_response:
                    if not retry_function(response.status, await response.json()):
                        raise
                    response = await response.json()
                else:
                    if not retry_function(response.status, await response.read()):
                        raise
                    response = await response.read()
                break
            except Exception as error:
                await self.reset()
        else:
            logger.warning('Retry and Fail of %s', url)
            response = None

        return response