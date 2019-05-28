import json

import pytest
from plenum.common.request import Request

from plenum.common.constants import AML
from plenum.common.exceptions import InvalidClientRequest


def test_taa_acceptance_static_validation(config_req_handler, taa_aml_request):
    taa_aml_request = json.loads(taa_aml_request)
    taa_aml_request['operation'][AML] = {}

    with pytest.raises(InvalidClientRequest) as e:
        config_req_handler.doStaticValidation(Request(**taa_aml_request))
    assert e.match('TXN_AUTHOR_AGREEMENT_AML request must contain at least one acceptance mechanism')


def test_taa_acceptance_dynamic_validation(config_req_handler, taa_aml_request):
    req = json.loads(taa_aml_request)
    config_req_handler.update_txn_author_agreement_acceptance_mechanisms(req['operation'], 1, 1)
    config_req_handler.authorize = lambda req: 0
    with pytest.raises(InvalidClientRequest) as e:
        config_req_handler.validate(Request(**req))
    assert e.match('Version of TAA AML must be unique and it cannot be modified')


def test_taa_acceptance_pass_validation(config_req_handler, taa_aml_request):
    config_req_handler.authorize = lambda req: 0
    req = Request(**json.loads(taa_aml_request))
    config_req_handler.doStaticValidation(req)
    config_req_handler.validate(req)
