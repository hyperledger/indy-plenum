import pytest
from plenum.common.types import f
from plenum.common.constants import TXN_PAYLOAD, TXN_PAYLOAD_METADATA
from plenum.test.helper import sdk_get_and_check_replies, sdk_sign_and_submit_req

SEC_PER_DAY = 24 * 60 * 60


@pytest.fixture(scope='module', autouse=True)
def activate_taa(activate_taa):
    return activate_taa


def _check_taa_time_correct(taa_acceptance):
    assert taa_acceptance[f.TAA_ACCEPTANCE.nm][f.TAA_ACCEPTANCE_TIME.nm] % SEC_PER_DAY == 0


def test_request_with_invalid_taa_acceptance_time(set_txn_author_agreement,
                                                  add_taa_acceptance,
                                                  sdk_wallet_new_steward,
                                                  sdk_pool_handle,
                                                  looper):
    taa_data = set_txn_author_agreement()
    request_json = add_taa_acceptance(
        taa_text=taa_data.text,
        taa_version=taa_data.version,
        taa_a_time=taa_data.txn_time + (0 if taa_data.txn_time % SEC_PER_DAY != 0 else 1)
    )

    req = sdk_sign_and_submit_req(sdk_pool_handle, sdk_wallet_new_steward, request_json)
    resp = sdk_get_and_check_replies(looper, [req])
    _check_taa_time_correct(resp[0][0])
    _check_taa_time_correct(resp[0][1]["result"][TXN_PAYLOAD][TXN_PAYLOAD_METADATA])
