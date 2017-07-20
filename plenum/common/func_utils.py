

def flatten(some_list):
    """
    [[1,2],[3,4]] => [1,2,3,4]
    """
    for inner_list in some_list:
        for item in inner_list:
            yield item


def decons(some_list):
    """
    [1,2,3,4] => (1, [2,3,4])
    """
    if len(some_list) < 2:
        return None
    return some_list[0], some_list[1:]
