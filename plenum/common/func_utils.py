from collections import Counter


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

    # using Counter since it has optimizations
    return Counter(some_list).most_common()
