import pytest

from plenum.common.exceptions import RequestNackedException, RequestRejectedException
from plenum.test.helper import sdk_sign_and_submit_req_obj, sdk_get_and_check_replies

from plenum.common.constants import AML


def test_taa_acceptance_writes(looper, taa_aml_request, sdk_pool_handle, sdk_wallet_trustee):
    req = sdk_sign_and_submit_req_obj(looper, sdk_pool_handle, sdk_wallet_trustee, taa_aml_request)
    sdk_get_and_check_replies(looper, [req])


def test_taa_acceptance_writes_module_static(looper, taa_aml_request, sdk_pool_handle, sdk_wallet_trustee):
    taa_aml_request.operation[AML] = {}
    req = sdk_sign_and_submit_req_obj(looper, sdk_pool_handle, sdk_wallet_trustee, taa_aml_request)
    with pytest.raises(RequestNackedException) as e:
        sdk_get_and_check_replies(looper, [req])
    assert e.match('TXN_AUTHOR_AGREEMENT_AML request must contain at least one acceptance mechanism')


def test_taa_acceptance_writes_module_dynamic(looper, taa_aml_request, sdk_pool_handle, sdk_wallet_trustee):
    req = sdk_sign_and_submit_req_obj(looper, sdk_pool_handle, sdk_wallet_trustee, taa_aml_request)
    sdk_get_and_check_replies(looper, [req])

    taa_aml_request.reqId = 111
    req = sdk_sign_and_submit_req_obj(looper, sdk_pool_handle, sdk_wallet_trustee, taa_aml_request)
    with pytest.raises(RequestRejectedException) as e:
        sdk_get_and_check_replies(looper, [req])
    assert e.match('Version of TAA AML must be unique and it cannot be modified')
