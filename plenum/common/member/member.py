
from plenum.common.constants import NYM, TARGET_NYM, ROLE, VERKEY, ALIAS
from plenum.common.txn_util import init_empty_txn, set_payload_data, append_payload_metadata


class Member:
    """
    Base class for different network member contexts.
    """
    @staticmethod
    def nym_txn(nym, name, verkey=None, role=None, creator=None):
        txn = init_empty_txn(NYM)

        txn_data = {
            TARGET_NYM: nym,
        }
        if verkey is not None:
            txn_data[VERKEY] = verkey
        if role is not None:
            txn_data[ROLE] = role
        if name is not None:
            txn_data[ALIAS] = name
        set_payload_data(txn, txn_data)

        txn = append_payload_metadata(txn,
                                   frm=creator)
        # txn = append_txn_metadata(txn,
        #                           txn_id=sha256(name.encode()).hexdigest())
        return txn
