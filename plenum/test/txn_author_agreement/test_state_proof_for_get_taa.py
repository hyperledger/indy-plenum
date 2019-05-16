from plenum.common.constants import REPLY
from plenum.common.util import randomString
from plenum.test.txn_author_agreement.helper import sdk_send_txn_author_agreement, sdk_get_txn_author_agreement, \
    taa_digest, check_state_proof


# TODO: Change txnPoolNodeSet to nodeSetWithOneNodeResponding after
#  correct state proof checking is implemented in Indy SDK
def test_state_proof_returned_for_get_txn_author_agreement(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_trustee, sdk_wallet_client):
    text = randomString(1024)
    version = randomString(16)
    sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_trustee, text, version)

    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client)[1]
    assert reply['op'] == REPLY

    result = reply['result']
    check_state_proof(result, '2:latest', taa_digest(text, version))
