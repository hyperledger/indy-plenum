import base58
import pytest

from common.serializers.serialization import state_roots_serializer
from crypto.bls.bls_multi_signature import MultiSignature, MultiSignatureValue
from crypto.bls.indy_crypto.bls_crypto_indy_crypto import \
    BlsCryptoSignerIndyCrypto
from indy_common.constants import NODE
from plenum.common.constants import VALIDATOR, BLS_KEY, BLS_KEY_PROOF, TXN_TYPE, \
    DATA
from plenum.common.exceptions import RequestNackedException, \
    InvalidClientRequest
from plenum.common.request import Request
from plenum.common.util import get_utc_epoch
from plenum.test.pool_transactions.helper import prepare_new_node_data
from stp_core.types import Identifier


@pytest.fixture(scope="function")
def pool_req_handler(txnPoolNodeSet):
    return txnPoolNodeSet[0].poolManager.reqHandler


@pytest.fixture(scope="function")
def bls_keys(tconf, tdir):
    new_node_name = "NewNode"
    _, _, bls_key, _, _, _, _, key_proof = prepare_new_node_data(tconf,
                                                                 tdir,
                                                                 new_node_name)
    return bls_key, key_proof


def test_pool_req_handler_static_validation(bls_keys,
                                            pool_req_handler):
    bls_ver_key, key_proof = bls_keys
    node_request = _generate_node_request(bls_key=bls_ver_key,
                                          bls_key_proof=key_proof)
    pool_req_handler.doStaticValidation(node_request)


def test_pool_req_handler_static_validation_with_full_bls(bls_keys,
                                                          pool_req_handler):
    bls_ver_key, key_proof = bls_keys
    node_request = _generate_node_request(bls_key=None,
                                          bls_key_proof=key_proof)
    pool_req_handler.doStaticValidation(node_request)


def test_pool_req_handler_static_validation_with_incorrect_proof(bls_keys,
                                                                 pool_req_handler):
    bls_ver_key, key_proof = bls_keys
    node_request = _generate_node_request(bls_key=bls_ver_key,
                                          bls_key_proof=key_proof.upper())
    with pytest.raises(InvalidClientRequest) as e:
        pool_req_handler.doStaticValidation(node_request)
        assert "Proof of possession {} " \
               "is incorrect for BLS key {}".format(key_proof, bls_ver_key) \
               in e._excinfo[1].args[0]


def test_pool_req_handler_static_validation_with_full_proof(bls_keys,
                                                            pool_req_handler):
    bls_ver_key, key_proof = bls_keys
    node_request = _generate_node_request(bls_key=bls_ver_key,
                                          bls_key_proof=None)
    with pytest.raises(InvalidClientRequest) as e:
        pool_req_handler.doStaticValidation(node_request)
        assert "A Proof of possession must be provided with BLS key" \
               in e._excinfo[1].args[0]


def _generate_node_request(bls_key=None,
                           bls_key_proof=None) -> Request:
    op = {
        DATA: {
            BLS_KEY: bls_key,
            BLS_KEY_PROOF: bls_key_proof
        },
        TXN_TYPE: NODE
    }
    return Request(operation=op,
                   reqId=123,
                   identifier=Identifier("idr"))
