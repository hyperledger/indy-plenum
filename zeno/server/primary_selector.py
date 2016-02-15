import logging
from typing import List

from zeno.common.util import getlogger
from zeno.server import replica
from zeno.server.primary_decider import PrimaryDecider

logger = getlogger()


class PrimarySelector(PrimaryDecider):
    def __init__(self, node):
        super().__init__(node)
        self.nodeNamesByRank = sorted(self.nodeNames)

    def decidePrimaries(self):  # overridden method of PrimaryDecider
        self.startSelection()

    def startSelection(self):
        logger.debug("{} starting selection".format(self))
        for idx, r in enumerate(self.replicas):
            prim = (self.viewNo + idx) % self.nodeCount
            primaryName = replica.Replica.generateName(
                self.nodeNamesByRank[prim],
                idx)
            logging.debug("{} has primary {}".format(r.name, primaryName))
            r.primaryName = primaryName

    def viewChanged(self, viewNo: int):
        if viewNo > self.viewNo:
            self.viewNo = viewNo
            self.startSelection()
        else:
            logging.warning("Provided view no {} is not greater than the "
                            "current view no {}".format(viewNo, self.viewNo))
