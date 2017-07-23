from collections import Counter


def split_at(some_list, num):
    """
    [a, b, c, d], num=1 => ([a], [b, c, d])
    [a, b, c, d], num=2 => ([a, b], [c, d])
    """
    head = some_list[:num]
    tail = some_list[num:]
    return head, tail


def count(some_list):
    """
    [a, b, c, a, b, b] => {b: 3, a: 2, c: 1}
    """

    # using Counter since it has optimizations
    return Counter(some_list).most_common()
