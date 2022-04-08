def split_chunk(list, n=100):
    for i in range(0, len(list), n):
        yield list[i: i + n]