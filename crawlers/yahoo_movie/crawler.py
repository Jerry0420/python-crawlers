import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../")

import asyncio
from utils.http_utils import AsyncRequestUtil

async def main():
    session = AsyncRequestUtil()
    response = await session.get(url="https://httpbin.org/get", json_response=True)
    print(response)
    await session.close()

if __name__ == "__main__":
    asyncio.run(main())