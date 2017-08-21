
from plenum.common.constants import TXN_TYPE, NYM, TARGET_NYM, TXN_ID, ROLE, VERKEY
from plenum.common.types import f


class Member:
    """
    Base class for different network member contexts.
    """
    @staticmethod
    def nym_txn(nym, name, verkey=None, role=None, creator=None):
        txn = {
            TXN_TYPE: NYM,
            TARGET_NYM: nym,
            # TXN_ID: sha256(name.encode()).hexdigest()
        }
        if verkey is not None:
            txn[VERKEY] = verkey
        if creator is not None:
            txn[f.IDENTIFIER.nm] = creator
        if role is not None:
            txn[ROLE] = role
        return txn
