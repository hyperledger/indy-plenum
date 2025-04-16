import pytest
from plenum.test.helper import vdr_send_random_requests, vdr_get_and_check_replies


@pytest.fixture(scope="function", autouse=True)
def limitTestRunningTime():
    return 300


def test_sdk_many_stewards_send_many(looper, vdr_pool_handle, vdr_wallet_stewards):
    for sdk_wallet_steward in vdr_wallet_stewards:
        resp_task = vdr_send_random_requests(looper, vdr_pool_handle, sdk_wallet_steward, 30)
        repl = vdr_get_and_check_replies(looper, resp_task, timeout=90)
        for _, resp in repl:
            assert resp['result']