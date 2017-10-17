from hashlib import sha256

from plenum.common.exceptions import WalletNotSet, WalletNotInitialized
from plenum.common.member.member import Member
from plenum.common.constants import STEWARD, TARGET_NYM, TXN_TYPE, NODE, DATA, \
    CLIENT_IP, ALIAS, CLIENT_PORT, NODE_IP, NODE_PORT, SERVICES, VALIDATOR, \
    TXN_ID, BLS_KEY
from plenum.common.types import f
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
                             blskey=self.node.blskey)

    @staticmethod
    def node_txn(steward_nym, node_name, nym, ip, node_port, client_port,
                 client_ip=None, blskey=None):
        txn = {
            TARGET_NYM: nym,
            TXN_TYPE: NODE,
            f.IDENTIFIER.nm: steward_nym,
            DATA: {
                CLIENT_IP: client_ip or ip,
                ALIAS: node_name,
                CLIENT_PORT: client_port,
                NODE_IP: ip,
                NODE_PORT: node_port,
                SERVICES: [VALIDATOR],
                BLS_KEY: blskey
            },
            TXN_ID: sha256(node_name.encode()).hexdigest()
        }
        return txn
