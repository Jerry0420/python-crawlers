import random
import json
from enum import Enum
from typing import List, Dict

class Countries(Enum):
    # TODO: add countries
    TW = 'tw'

def get_proxy(country_list: List[str]) -> Dict[str, str]:
    data = None
    proxy = None
    try:
        with open('proxies.json', 'r') as json_file:
            data = json.load(json_file)
        country = random.choice(country_list)
        proxies = data[country]
        proxy = random.choice(proxies)
    except Exception as error:
        pass
    
    return proxy