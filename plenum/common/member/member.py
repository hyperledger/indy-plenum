from plenum.common.constants import NYM, TARGET_NYM, ROLE, VERKEY, ALIAS, CURRENT_PROTOCOL_VERSION
from plenum.common.txn_util import init_empty_txn, set_payload_data, append_payload_metadata, append_txn_metadata


class Member:
    """
    Base class for different network member contexts.
    """

    @staticmethod
    def nym_txn(nym, name=None, verkey=None, role=None, creator=None, txn_id=None, seq_no=None,
                protocol_version=CURRENT_PROTOCOL_VERSION):
        txn = init_empty_txn(NYM, protocol_version=protocol_version)

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
        if txn_id:
            txn = append_txn_metadata(txn, txn_id=txn_id)
        if seq_no:
            txn = append_txn_metadata(txn, seq_no=seq_no)

        return txn
