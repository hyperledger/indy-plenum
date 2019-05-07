from plenum.common.constants import TXN_AUTHOR_AGREEMENT_TEXT, TXN_AUTHOR_AGREEMENT_VERSION
from plenum.server.config_req_handler import ConfigReqHandler
from plenum.test.txn_author_agreement.helper import get_config_req_handler


def test_state_path_taa_latest():
    assert ConfigReqHandler._state_path_taa_latest() == b'taa:v:latest'


def test_state_path_taa_version():
    assert ConfigReqHandler._state_path_taa_version('some_version') == b'taa:v:some_version'


def test_state_path_taa_digest():
    assert ConfigReqHandler._state_path_taa_digest('some_digest') == b'taa:d:some_digest'


def test_taa_digest():
    assert ConfigReqHandler._taa_digest('some_version', 'some_text') == \
           "fb2ea9d28380a021ec747c442d62a68952b4b5813b45671098ad2b684b2f4646"


def test_is_trustee(txnPoolNodeSet, sdk_wallet_trustee, sdk_wallet_steward, sdk_wallet_client):
    config_req_handler = get_config_req_handler(txnPoolNodeSet[0])

    assert config_req_handler._is_trustee(sdk_wallet_trustee[1])
    assert not config_req_handler._is_trustee(sdk_wallet_steward[1])
    assert not config_req_handler._is_trustee(sdk_wallet_client[1])


def test_update_txn_author_agreement(config_req_handler: ConfigReqHandler):
    text_v1 = 'Some agreement'
    agreement_v1 = {TXN_AUTHOR_AGREEMENT_TEXT: text_v1, TXN_AUTHOR_AGREEMENT_VERSION: 'v1'}
    digest_v1 = ConfigReqHandler._taa_digest('v1', text_v1).encode()
    config_req_handler.update_txn_author_agreement(agreement_v1)

    assert config_req_handler.get_taa_digest(isCommitted=False) == digest_v1
    assert config_req_handler.get_taa_digest(version='v1', isCommitted=False) == digest_v1
    assert config_req_handler.get_taa_digest(version='v2', isCommitted=False) is None

    text_v2 = 'New agreement'
    agreement_v2 = {TXN_AUTHOR_AGREEMENT_TEXT: text_v2, TXN_AUTHOR_AGREEMENT_VERSION: 'v2'}
    digest_v2 = ConfigReqHandler._taa_digest('v2', text_v2).encode()
    config_req_handler.update_txn_author_agreement(agreement_v2)

    assert config_req_handler.get_taa_digest(isCommitted=False) == digest_v2
    assert config_req_handler.get_taa_digest(version='v1', isCommitted=False) == digest_v1
    assert config_req_handler.get_taa_digest(version='v2', isCommitted=False) == digest_v2
