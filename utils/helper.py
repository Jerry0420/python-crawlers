from typing import List, Any

def split_chunk(list: List[Any], n=100):
    for i in range(0, len(list), n):
        yield list[i: i + n]