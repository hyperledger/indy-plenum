import json
import pytest

from plenum.common.request import Request
from plenum.common.constants import TXN_AUTHOR_AGREEMENT_AML, \
    AML_VERSION, AML, AML_CONTEXT, CURRENT_PROTOCOL_VERSION
from plenum.common.exceptions import RequestNackedException, RequestRejectedException, InvalidClientRequest
from plenum.common.util import randomString
from plenum.test.helper import sdk_get_and_check_replies, sdk_sign_and_submit_req_obj


@pytest.fixture(scope="function")
def taa_aml_request(sdk_wallet_trustee):
    return Request(identifier=sdk_wallet_trustee[1],
                   reqId=5,
                   protocolVersion=CURRENT_PROTOCOL_VERSION,
                   operation={'type': TXN_AUTHOR_AGREEMENT_AML,
                              AML_VERSION: randomString(),
                              AML: json.dumps({'Nice way': 'very good way to accept agreement'}),
                              AML_CONTEXT: randomString()})


def test_taa_acceptance_static_validation(config_req_handler, taa_aml_request):
    taa_aml_request.operation[AML] = '{}'
    with pytest.raises(InvalidClientRequest) as e:
        config_req_handler.doStaticValidation(taa_aml_request)
    assert e.match('TAA AML request must contain at least one acceptance mechanism')


def test_taa_acceptance_dynamic_validation(config_req_handler, taa_aml_request):
    config_req_handler.update_txn_author_agreement_acceptance_mechanisms(taa_aml_request.operation)
    config_req_handler.authorize = lambda req: 0
    with pytest.raises(InvalidClientRequest) as e:
        config_req_handler.validate(taa_aml_request)
    assert e.match('Version of TAA AML must be unique and it cannot be modified')


def test_taa_acceptance_pass_validation(config_req_handler, taa_aml_request):
    config_req_handler.authorize = lambda req: 0
    config_req_handler.doStaticValidation(taa_aml_request)
    config_req_handler.validate(taa_aml_request)


def test_taa_acceptance_writes_module_static(looper, taa_aml_request, sdk_pool_handle, sdk_wallet_trustee):
    taa_aml_request.operation[AML] = '{}'
    req = sdk_sign_and_submit_req_obj(looper, sdk_pool_handle, sdk_wallet_trustee, taa_aml_request)
    with pytest.raises(RequestNackedException) as e:
        sdk_get_and_check_replies(looper, [req])
    assert e.match('TAA AML request must contain at least one acceptance mechanism')


def test_taa_acceptance_writes_module_dynamic(looper, taa_aml_request, sdk_pool_handle, sdk_wallet_trustee):
    req = sdk_sign_and_submit_req_obj(looper, sdk_pool_handle, sdk_wallet_trustee, taa_aml_request)
    sdk_get_and_check_replies(looper, [req])

    taa_aml_request.reqId = 111
    req = sdk_sign_and_submit_req_obj(looper, sdk_pool_handle, sdk_wallet_trustee, taa_aml_request)
    with pytest.raises(RequestRejectedException) as e:
        sdk_get_and_check_replies(looper, [req])
    assert e.match('Version of TAA AML must be unique and it cannot be modified')
