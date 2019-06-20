from hashlib import sha256

from plenum.common.constants import STEWARD, TARGET_NYM, NODE, DATA, \
    CLIENT_IP, ALIAS, CLIENT_PORT, NODE_IP, NODE_PORT, SERVICES, VALIDATOR, \
    BLS_KEY, CURRENT_PROTOCOL_VERSION, BLS_KEY_PROOF
from plenum.common.exceptions import WalletNotSet, WalletNotInitialized
from plenum.common.member.member import Member
from plenum.common.txn_util import init_empty_txn, append_payload_metadata, set_payload_data, append_txn_metadata
from plenum.common.util import hexToFriendly


class Steward(Member):
    """
    Provides a context for Steward operations.
    """

    def __init__(self, name=None, wallet=None):
        self.name = name or 'Steward' + str(id(self))
        self._wallet = wallet
        self.node = None

    @property
    def wallet(self):
        if not self._wallet:
            raise WalletNotSet
        return self._wallet

    @property
    def nym(self):
        if not self.wallet.defaultId:
            raise WalletNotInitialized
        return self.wallet.defaultId

    def set_node(self, node, **kwargs):
        self.node = node.copy()
        if kwargs:
            self.node.update(kwargs)

    def generate_genesis_txns(self):
        nym_txn = self._nym_txn()
        node_txn = self._node_txn()
        return [nym_txn, node_txn]

    def _nym_txn(self, creator=None):
        return self.nym_txn(self.nym, self.name,
                            verkey=self.wallet.getVerkey(self.nym),
                            role=STEWARD, creator=creator)

    def _node_txn(self):
        node_nym = hexToFriendly(self.node.verkey)
        return self.node_txn(steward_nym=self.nym,
                             node_name=self.node.name,
                             nym=node_nym,
                             ip=self.node.ha[0],
                             node_port=self.node.ha[1],
                             client_ip=self.node.cliha[0],
                             client_port=self.node.cliha[1],
                             blskey=self.node.blskey,
                             bls_key_proof=self.node.blsley_proof)

    @staticmethod
    def node_txn(steward_nym, node_name, nym, ip, node_port, client_port,
                 client_ip=None, blskey=None, services=None, txn_id=None,
                 seq_no=None,
                 protocol_version=CURRENT_PROTOCOL_VERSION, bls_key_proof=None):
        txn = init_empty_txn(txn_type=NODE, protocol_version=protocol_version)
        txn = append_payload_metadata(txn, frm=steward_nym)
        txn_data = {
            TARGET_NYM: nym,
            DATA: {
                CLIENT_IP: client_ip or ip,
                ALIAS: node_name,
                CLIENT_PORT: client_port,
                NODE_IP: ip,
                NODE_PORT: node_port,
                SERVICES: services or [VALIDATOR],
                BLS_KEY: blskey,
                BLS_KEY_PROOF: bls_key_proof
            },
        }
        txn = set_payload_data(txn, txn_data)
        txn_id = txn_id or sha256(node_name.encode()).hexdigest()
        txn = append_txn_metadata(txn, txn_id=txn_id)
        if seq_no:
            txn = append_txn_metadata(txn, seq_no=seq_no)
        return txn
