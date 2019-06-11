from common.serializers.serialization import domain_state_serializer
from plenum.common.constants import ROLE, TXN_TYPE, NYM, TARGET_NYM
from plenum.common.request import Request
from plenum.common.txn_util import reqToTxn
from plenum.server.request_handlers.utils import nym_to_state_key


def create_nym_txn(identifier, role, nym="TARGET_NYM"):
    return reqToTxn(Request(identifier=identifier,
                            operation={ROLE: role,
                                       TXN_TYPE: NYM,
                                       TARGET_NYM: nym}))


def update_nym(state, identifier, role):
    state.set(nym_to_state_key(identifier),
              domain_state_serializer.serialize(
                  create_nym_txn(identifier, role)['txn']['data']))
