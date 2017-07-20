

def flatten(some_list):
    """
    [[a,b],[c,d]] => [a,b,c,d]
    """
    for inner_list in some_list:
        for item in inner_list:
            yield item


def decons(some_list):
    """
    [a,b,c,d] => (a, [b,c,d])
    """
    if len(some_list) < 2:
        return None
    return some_list[0], some_list[1:]


def count(some_list):
    """
    [a,b,c,a,b,b] => {a: 2, b: 3, c: 1}
    """
    votes = {}
    for item in some_list:
        votes[item] = 1 if item not in votes else votes[item] + 1
    return sorted(votes.items(), key=lambda item: item[1], reverse=True)
