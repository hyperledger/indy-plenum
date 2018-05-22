db = {}
hashes = {}
hash_ = hash


def hash(x):
    h = hash_(x)
    hashes[x] = h
    return h


def update(node,path,value):
    curnode = db.get(node) if node else [''] * 17
    newnode = curnode[:]
    if path == '':
        newnode[-1] = value
    else:
        cur_letter = int(path[0])
        newindex = update(curnode[cur_letter],path[1:],value)
        newnode[cur_letter] = newindex
    h = hash(tuple(newnode))
    db[h] = newnode
    return h


def delete(node,path):
    if node is '':
        return ''
    else:
        curnode = db.get(node)
        newnode = curnode[:]
        if path == '':
            newnode[-1] = ''
        else:
            cur_letter = int(path[0])
            newindex = delete(curnode[cur_letter],path[1:])
            newnode[cur_letter] = newindex

        if len([x for x in newnode if x != '']) == 0:
            return ''
        else:
            h = hash(tuple(newnode))
            db[h] = newnode
            return h


r = update('', '1', 'a')
r = update(r, '2', 'b')
r = update(r, '21', 'c')
r = update(r, '11', 'b')
r = update(r, '211', 'd')
