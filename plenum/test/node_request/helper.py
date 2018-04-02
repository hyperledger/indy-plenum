import operator


def nodes_by_rank(txnPoolNodeSet):
    return [t[1] for t in sorted([(node.rank, node)
                                  for node in txnPoolNodeSet],
                                 key=operator.itemgetter(0))]
