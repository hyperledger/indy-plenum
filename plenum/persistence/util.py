
from copy import deepcopy

from ledger.util import F
from plenum.common.util import pop_keys


def pop_merkle_info(txn):
    pop_keys(txn, lambda k: k in (F.auditPath.name, F.rootHash.name))
