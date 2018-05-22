class KeyValueStorageInMemory:
    def __init__(self):
        self._dict = {}

    def get(self, key):
        if isinstance(key, str):
            key = key.encode()
        return self._dict[key]

    def put(self, key, value):
        if isinstance(key, str):
            key = key.encode()
        if isinstance(value, str):
            value = value.encode()
        self._dict[key] = value


db = KeyValueStorageInMemory()


def update(node, path, value):
    if path == '':
        curnode = db.get(node) if node else [ '' ] * 17
        newnode = curnode.copy()
        newnode[-1] = value
    else:
        curnode = db.get(node) if node else [ '' ] * 17
        newnode = curnode.copy()
        newindex = update(curnode[int(path[0])], path[1:], value)
        newnode[int(path[0])] = newindex
    h = hash(tuple(newnode))
    db.put(h, newnode)
    return h


def test1():
    r1 = update('', '11', 'v1')
    r2 = update('', '12', 'v2')
    r3 = update('', '13', 'v3')
    print(1)