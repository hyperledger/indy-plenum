
def decons(some_list):
    """
    [a, b, c, d] => (a, [b, c, d])
    """
    if len(some_list) < 2:
        return None
    return some_list[0], some_list[1:]


def count(some_list):
    """
    [a, b, c, a, b, b] => {b: 3, a: 2, c: 1}
    """
    counters = {}
    for item in some_list:
        counters[item] = 1 if item not in counters else counters[item] + 1
    return sorted(counters.items(), key=lambda item: item[1], reverse=True)
