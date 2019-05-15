import base64

import base58

from plenum.common.constants import REPLY, STATE_PROOF, ROOT_HASH, PROOF_NODES
from plenum.common.util import randomString
from plenum.test.txn_author_agreement.helper import sdk_send_txn_author_agreement, sdk_get_txn_author_agreement, \
    check_valid_proof, taa_digest
from state.pruning_state import PruningState


# TODO: Change txnPoolNodeSet to nodeSetWithOneNodeResponding after
#  correct state proof checking is implemented in Indy SDK
def test_state_proof_returned_for_get_txn_author_agreement(
        looper, setup, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_trustee, sdk_wallet_client):
    text = randomString(1024)
    version = randomString(16)
    sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_trustee, text, version)

    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client)[1]
    assert reply['op'] == REPLY

    result = reply['result']
    check_valid_proof(result)

    state_proof = result[STATE_PROOF]
    proof_nodes = base64.b64decode(state_proof[PROOF_NODES])
    root_hash = base58.b58decode(state_proof[ROOT_HASH])

    assert PruningState.verify_state_proof(root_hash,
                                           '2:latest',
                                           taa_digest(text, version),
                                           proof_nodes, serialized=True)
