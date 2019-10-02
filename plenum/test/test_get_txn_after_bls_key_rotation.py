from plenum.test.bls.helper import check_update_bls_key
from indy import ledger
from indy.did import create_and_store_my_did

from plenum.test.delayers import cDelay
from plenum.test.stasher import delay_rules, delay_rules_without_processing

nodeCount = 4


def test_get_txn_after_bls_key_rotation(looper, txnPoolNodeSet,
                                        sdk_wallet_stewards,
                                        sdk_wallet_trustee,
                                        sdk_wallet_client,
                                        sdk_pool_handle):
    check_update_bls_key(node_num=0, saved_multi_sigs_count=4,
                         looper=looper, txnPoolNodeSet=txnPoolNodeSet,
                         sdk_wallet_stewards=sdk_wallet_stewards,
                         sdk_wallet_client=sdk_wallet_client,
                         sdk_pool_handle=sdk_pool_handle,
                         pool_refresh=False)
    check_update_bls_key(node_num=1, saved_multi_sigs_count=4,
                         looper=looper, txnPoolNodeSet=txnPoolNodeSet,
                         sdk_wallet_stewards=sdk_wallet_stewards,
                         sdk_wallet_client=sdk_wallet_client,
                         sdk_pool_handle=sdk_pool_handle,
                         pool_refresh=False)
    check_update_bls_key(node_num=2, saved_multi_sigs_count=4,
                         looper=looper, txnPoolNodeSet=txnPoolNodeSet,
                         sdk_wallet_stewards=sdk_wallet_stewards,
                         sdk_wallet_client=sdk_wallet_client,
                         sdk_pool_handle=sdk_pool_handle,
                         pool_refresh=False)
    check_update_bls_key(node_num=3, saved_multi_sigs_count=4,
                         looper=looper, txnPoolNodeSet=txnPoolNodeSet,
                         sdk_wallet_stewards=sdk_wallet_stewards,
                         sdk_wallet_client=sdk_wallet_client,
                         sdk_pool_handle=sdk_pool_handle,
                         pool_refresh=False)

    # Stop receiving of commits in a circle, so all nodes will have different sets of multi signatures
    with delay_rules_without_processing(txnPoolNodeSet[0].nodeIbStasher, cDelay(delay=1200, sender_filter=txnPoolNodeSet[3].name)):
        with delay_rules_without_processing(txnPoolNodeSet[1].nodeIbStasher, cDelay(delay=1200, sender_filter=txnPoolNodeSet[0].name)):
            with delay_rules_without_processing(txnPoolNodeSet[2].nodeIbStasher, cDelay(delay=1200, sender_filter=txnPoolNodeSet[1].name)):
                with delay_rules_without_processing(txnPoolNodeSet[3].nodeIbStasher, cDelay(delay=1200, sender_filter=txnPoolNodeSet[2].name)):
                    did_future = create_and_store_my_did(sdk_wallet_client[0], "{}")
                    did, verkey = looper.loop.run_until_complete(did_future)
                    nym_request_future = ledger.build_nym_request(sdk_wallet_trustee[1], did, verkey, None, None)
                    nym_request = looper.loop.run_until_complete(nym_request_future)
                    nym_response_future = ledger.sign_and_submit_request(sdk_pool_handle, sdk_wallet_trustee[0], sdk_wallet_trustee[1], nym_request)
                    looper.loop.run_until_complete(nym_response_future)

                    get_txn_request_future = ledger.build_get_txn_request(sdk_wallet_client[1], "DOMAIN", 1)
                    get_txn_request = looper.loop.run_until_complete(get_txn_request_future)
                    get_txn_response_future = ledger.submit_request(sdk_pool_handle, get_txn_request)
                    looper.loop.run_until_complete(get_txn_response_future)
