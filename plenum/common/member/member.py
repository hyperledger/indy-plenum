from hashlib import sha256

from plenum.common.txn import TXN_TYPE, NYM, TARGET_NYM, TXN_ID, ROLE
from plenum.common.types import f


class Member:
    """
    Base class for different network member contexts.
    """
    @staticmethod
    def nym_txn(nym, name, role=None, creator=None):
        txn = {
            TXN_TYPE: NYM,
            TARGET_NYM: nym,
            TXN_ID: sha256(name.encode()).hexdigest()
        }
        if creator is not None:
            txn[f.IDENTIFIER.nm] = creator
        if role is not None:
            txn[ROLE] = role
        return txn
