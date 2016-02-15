from typing import Iterable

from zeno.test.helper import TestNode

# TODO Will probably come back to this later.

def resetNodeStore(nodes: Iterable[TestNode]):
    for node in nodes:
        node.resetData()
