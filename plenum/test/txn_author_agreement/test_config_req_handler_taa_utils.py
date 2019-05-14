import json

from common.serializers.serialization import config_state_serializer

from plenum.common.constants import (
    TXN_AUTHOR_AGREEMENT_TEXT, TXN_AUTHOR_AGREEMENT_VERSION,
    TXN_PAYLOAD, TXN_METADATA, TXN_METADATA_SEQ_NO, TXN_METADATA_TIME
)
from plenum.server.config_req_handler import ConfigReqHandler
from plenum.test.txn_author_agreement.helper import (
    get_config_req_handler, expected_state_data
)


def test_state_path_taa_latest():
    assert ConfigReqHandler._state_path_taa_latest() == b'taa:v:latest'


def test_state_path_taa_version():
    assert ConfigReqHandler._state_path_taa_version('some_version') == b'taa:v:some_version'


def test_state_path_taa_digest():
    assert ConfigReqHandler._state_path_taa_digest('some_digest') == b'taa:d:some_digest'


def test_taa_digest():
    assert ConfigReqHandler._taa_digest('some_version', 'some_text') == \
        b"fb2ea9d28380a021ec747c442d62a68952b4b5813b45671098ad2b684b2f4646"


def test_is_trustee(txnPoolNodeSet, sdk_wallet_trustee, sdk_wallet_steward, sdk_wallet_client):
    config_req_handler = get_config_req_handler(txnPoolNodeSet[0])

    assert config_req_handler._is_trustee(sdk_wallet_trustee[1])
    assert not config_req_handler._is_trustee(sdk_wallet_steward[1])
    assert not config_req_handler._is_trustee(sdk_wallet_client[1])


def test_update_txn_author_agreement(
        config_req_handler: ConfigReqHandler, taa_in_data, taa_digests):
    """ `update_txn_author_agreement` updates state properly """
    data = taa_in_data[0]

    config_req_handler.update_txn_author_agreement(
        data.version, data.text, data.seq_no, data.txn_time)
    state = config_req_handler.state

    digest = taa_digests[data.version]

    assert state.get(ConfigReqHandler._state_path_taa_latest(),
                     isCommitted=False) == digest.encode()
    assert state.get(ConfigReqHandler._state_path_taa_version(data.version),
                     isCommitted=False) == digest.encode()

    taa = state.get(ConfigReqHandler._state_path_taa_digest(digest),
                    isCommitted=False)
    assert taa is not None

    taa = config_state_serializer.deserialize(taa)
    assert taa == expected_state_data(data)


def test_get_taa_digest(config_req_handler: ConfigReqHandler, taa_in_data, taa_state_data):
    """ `get_taa_digest` returns values from state """
    state = config_req_handler.state

    for data in taa_in_data:
        config_req_handler.update_txn_author_agreement(*data)

        assert (
            config_req_handler.get_taa_digest(isCommitted=False).encode() ==
            state.get(ConfigReqHandler._state_path_taa_latest(), isCommitted=False)
        )

        for version in taa_state_data:
            digest = config_req_handler.get_taa_digest(version=version, isCommitted=False)
            if digest is not None:
                digest = digest.encode()
            assert digest == state.get(
                ConfigReqHandler._state_path_taa_version(version), isCommitted=False)


def test_get_taa_data(config_req_handler: ConfigReqHandler,
                      taa_in_data, taa_state_data, taa_digests):
    """ `get_taa_data` returns values from state """
    state = config_req_handler.state

    def _state_data(digest=None, version=None):
        if digest is None:
            digest = config_req_handler.get_taa_digest(
                version=version, isCommitted=False)
        data = state.get(
            ConfigReqHandler._state_path_taa_digest(digest),
            isCommitted=False
        )
        if data is None:
            return None
        data = config_state_serializer.deserialize(data)
        return data['val'], data['lsn'], data['lut']

    for data in taa_in_data:
        config_req_handler.update_txn_author_agreement(*data)

        assert (
            config_req_handler.get_taa_data(isCommitted=False) == _state_data()
        )

        for version in taa_state_data:
            data = config_req_handler.get_taa_data(version=version, isCommitted=False)
            assert data == _state_data(version=version)

        for digest in taa_digests:
            data = config_req_handler.get_taa_data(
                digest=digest, version='any-version-since-ignored',
                isCommitted=False
            )
            assert data == _state_data(digest=digest)


def test_multiple_update_txn_author_agreement(
        config_req_handler: ConfigReqHandler, taa_in_data, taa_digests, taa_state_data):
    written = []
    for data in taa_in_data:
        digest = taa_digests[data.version]

        config_req_handler.update_txn_author_agreement(*data)
        written.append(data.version)

        assert config_req_handler.get_taa_digest(isCommitted=False) == digest
        _state_data = taa_state_data[data.version]
        _state_data = _state_data['val'], _state_data['lsn'], _state_data['lut']
        assert (
            config_req_handler.get_taa_data(isCommitted=False) ==
            _state_data
        )

        for _data in taa_in_data:
            _digest = taa_digests[_data.version]
            _state_data = taa_state_data[_data.version]
            _state_data = _state_data['val'], _state_data['lsn'], _state_data['lut']
            res = (
                config_req_handler.get_taa_digest(version=_data.version, isCommitted=False),
                config_req_handler.get_taa_data(version=_data.version, isCommitted=False),
                config_req_handler.get_taa_data(digest=_digest, isCommitted=False),
            )
            assert res == (
                (_digest, _state_data, _state_data) if _data.version in written else (None, None, None)
            )
