import json

import pytest
from plenum.test.pool_transactions.helper import sdk_sign_and_send_prepared_request

from plenum.common.exceptions import RequestNackedException, RequestRejectedException
from plenum.test.helper import sdk_get_and_check_replies

from plenum.common.constants import AML, AML_CONTEXT


def test_taa_acceptance_writes(looper, taa_aml_request, sdk_pool_handle, sdk_wallet_trustee):
    req = sdk_sign_and_send_prepared_request(looper, sdk_wallet_trustee, sdk_pool_handle, taa_aml_request)
    sdk_get_and_check_replies(looper, [req])


def test_taa_acceptance_writes_module_static(looper, taa_aml_request, sdk_pool_handle, sdk_wallet_trustee):
    taa_aml_request = json.loads(taa_aml_request)
    taa_aml_request['operation'][AML] = {}
    taa_aml_request = json.dumps(taa_aml_request)

    req = sdk_sign_and_send_prepared_request(looper, sdk_wallet_trustee, sdk_pool_handle, taa_aml_request)
    with pytest.raises(RequestNackedException) as e:
        sdk_get_and_check_replies(looper, [req])
    assert e.match('TXN_AUTHOR_AGREEMENT_AML request must contain at least one acceptance mechanism')


def test_taa_acceptance_writes_module_dynamic(looper, taa_aml_request, sdk_pool_handle, sdk_wallet_trustee):
    req = sdk_sign_and_send_prepared_request(looper, sdk_wallet_trustee, sdk_pool_handle, taa_aml_request)
    sdk_get_and_check_replies(looper, [req])

    taa_aml_request = json.loads(taa_aml_request)
    taa_aml_request['reqId'] = 111
    taa_aml_request = json.dumps(taa_aml_request)
    req = sdk_sign_and_send_prepared_request(looper, sdk_wallet_trustee, sdk_pool_handle, taa_aml_request)
    with pytest.raises(RequestRejectedException) as e:
        sdk_get_and_check_replies(looper, [req])
    assert e.match('Version of TAA AML must be unique and it cannot be modified')


def test_taa_aml_optional_description(looper, taa_aml_request, sdk_pool_handle, sdk_wallet_trustee):
    taa_aml_request = json.loads(taa_aml_request)
    del taa_aml_request['operation'][AML_CONTEXT]
    taa_aml_request = json.dumps(taa_aml_request)

    req = sdk_sign_and_send_prepared_request(looper, sdk_wallet_trustee, sdk_pool_handle, taa_aml_request)
    sdk_get_and_check_replies(looper, [req])
