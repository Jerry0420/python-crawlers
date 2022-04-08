import random
from enum import Enum
import json
from collections import namedtuple

class OS(Enum):
    # TODO: add os
    MACOS = "macos"

class Browser(Enum):
    # TODO: add browsers
    CHROME = "chrome"
    FIREFOX = "firefox"

UserAgentType = namedtuple('UserAgentType', ['os', 'browser'])

def get_user_agent(user_agent_type: UserAgentType) -> str:
    user_agent = ""
    try:
        with open('user_agents.json', 'r') as json_file:
            user_agents = json.load(json_file)
        user_agent = random.choice(user_agents[user_agent_type.os.value][user_agent_type.browser.value])
    except:
        user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"
    return user_agent