import json

import pytest
from plenum.common.exceptions import RequestNackedException

from plenum.test.helper import sdk_get_and_check_replies

from plenum.common.util import randomString
from plenum.common.constants import FORCE

from plenum.test.pool_transactions.helper import sdk_sign_and_send_prepared_request, prepare_nym_request, \
    sdk_add_new_nym


def test_forced_request_validation(looper, txnPoolNodeSet, sdk_wallet_client,
                                   sdk_pool_handle, sdk_wallet_steward):
    nym_request, new_did = looper.loop.run_until_complete(
        prepare_nym_request(sdk_wallet_client, randomString(32),
                            None, None))

    request_json = json.loads(nym_request)
    request_json['operation'][FORCE] = True
    node_request = json.dumps(request_json)

    request_couple = sdk_sign_and_send_prepared_request(looper,
                                                        sdk_wallet_client,
                                                        sdk_pool_handle,
                                                        node_request)

    with pytest.raises(RequestNackedException):
        sdk_get_and_check_replies(looper, [request_couple])

    sdk_add_new_nym(looper, sdk_pool_handle, sdk_wallet_steward)
