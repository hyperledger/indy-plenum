from common.serializers.serialization import domain_state_serializer
from plenum.common.constants import ROLE, TXN_TYPE, NYM, TARGET_NYM, TXN_AUTHOR_AGREEMENT_TEXT, \
    TXN_AUTHOR_AGREEMENT_VERSION, TXN_AUTHOR_AGREEMENT_RETIREMENT_TS, TXN_AUTHOR_AGREEMENT_DIGEST, \
    TXN_AUTHOR_AGREEMENT_RATIFICATION_TS
from plenum.common.request import Request
from plenum.common.txn_util import reqToTxn, get_payload_data, append_txn_metadata
from plenum.common.util import get_utc_epoch
from plenum.server.request_handlers.static_taa_helper import StaticTAAHelper
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


def check_taa_in_state(handler, digest, version, state_data):
    assert handler.get_from_state(
        StaticTAAHelper.state_path_taa_digest(digest)) == state_data
    assert handler.state.get(
        StaticTAAHelper.state_path_taa_version(version), isCommitted=False) == digest.encode()


def create_taa_txn(taa_request, taa_pp_time):
    taa_seq_no = 1
    taa_txn_time = taa_pp_time
    txn_id = "id"
    taa_txn = reqToTxn(taa_request)
    payload = get_payload_data(taa_txn)
    text = payload[TXN_AUTHOR_AGREEMENT_TEXT]
    version = payload[TXN_AUTHOR_AGREEMENT_VERSION]
    ratified = payload[TXN_AUTHOR_AGREEMENT_RATIFICATION_TS]
    retired = payload.get(TXN_AUTHOR_AGREEMENT_RETIREMENT_TS)
    digest = StaticTAAHelper.taa_digest(text, version)
    append_txn_metadata(taa_txn, taa_seq_no, taa_txn_time, txn_id)

    state_value = {TXN_AUTHOR_AGREEMENT_TEXT: text,
                   TXN_AUTHOR_AGREEMENT_VERSION: version,
                   TXN_AUTHOR_AGREEMENT_RATIFICATION_TS: ratified,
                   TXN_AUTHOR_AGREEMENT_DIGEST: digest}
    if retired:
        state_value[TXN_AUTHOR_AGREEMENT_RETIREMENT_TS] = retired
    return taa_txn, digest, (state_value, taa_seq_no, taa_txn_time)
