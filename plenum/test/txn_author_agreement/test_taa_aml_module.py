import json

import pytest
from plenum.common.request import Request

from plenum.common.constants import AML
from plenum.common.exceptions import InvalidClientRequest


def test_taa_acceptance_static_validation(write_manager, taa_aml_request):
    taa_aml_request = json.loads(taa_aml_request)
    taa_aml_request['operation'][AML] = {}

    with pytest.raises(InvalidClientRequest) as e:
        write_manager.static_validation(Request(**taa_aml_request))
    assert e.match('TXN_AUTHOR_AGREEMENT_AML request must contain at least one acceptance mechanism')


def test_taa_acceptance_dynamic_validation(taa_handler, taa_aml_handler, write_manager, taa_aml_request):
    req = json.loads(taa_aml_request)
    taa_aml_handler._update_txn_author_agreement_acceptance_mechanisms(req['operation'], 1, 1)
    taa_aml_handler.authorize = lambda req: 0
    with pytest.raises(InvalidClientRequest) as e:
        write_manager.dynamic_validation(Request(**req))
    assert e.match('Version of TAA AML must be unique and it cannot be modified')


def test_taa_acceptance_pass_validation(taa_aml_handler, write_manager, taa_aml_request):
    taa_aml_handler.authorize = lambda req: 0
    req = Request(**json.loads(taa_aml_request))
    write_manager.static_validation(req)
    write_manager.dynamic_validation(req)
