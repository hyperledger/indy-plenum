from common.serializers.serialization import config_state_serializer
from plenum.common.constants import AML_VERSION, AML, AML_CONTEXT, DOMAIN_LEDGER_ID, CONFIG_LEDGER_ID

from plenum.server.request_handlers.utils import VALUE, encode_state_value, is_trustee
from plenum.server.request_managers.write_request_manager import WriteRequestManager
from plenum.test.txn_author_agreement.helper import get_aml_req_handler


def test_state_path_taa_latest():
    assert WriteRequestManager._state_path_taa_latest() == b'2:latest'


def test_state_path_taa_version():
    assert WriteRequestManager._state_path_taa_version('some_version') == b'2:v:some_version'


def test_state_path_taa_digest():
    assert WriteRequestManager._state_path_taa_digest('some_digest') == b'2:d:some_digest'


def test_taa_digest():
    assert WriteRequestManager._taa_digest('some_text', 'some_version') == \
        "fb2ea9d28380a021ec747c442d62a68952b4b5813b45671098ad2b684b2f4646"


def test_state_path_taa_aml_latest():
    assert WriteRequestManager._state_path_taa_aml_latest() == b'3:latest'


def test_state_path_taa_aml_version():
    assert WriteRequestManager._state_path_taa_aml_version('some_version') == b'3:v:some_version'


def test_is_trustee(txnPoolNodeSet, sdk_wallet_trustee, sdk_wallet_steward, sdk_wallet_client):
    aml_req_handler = get_aml_req_handler(txnPoolNodeSet[0])
    state = aml_req_handler.database_manager.get_database(DOMAIN_LEDGER_ID).state
    assert is_trustee(state, sdk_wallet_trustee[1])
    assert not is_trustee(state, sdk_wallet_steward[1])
    assert not is_trustee(state, sdk_wallet_client[1])


def test_add_txn_author_agreement(taa_handler, write_manager, taa_input_data,
        taa_expected_state_data, taa_expected_digests
):
    """ `update_txn_author_agreement` updates state properly """
    state = write_manager.database_manager.get_state(CONFIG_LEDGER_ID)
    written = []

    def _check_state(version):
        digest = taa_expected_digests[version]

        _digest = state.get(
            WriteRequestManager._state_path_taa_version(version),
            isCommitted=False
        )
        _data = state.get(
            WriteRequestManager._state_path_taa_digest(digest),
            isCommitted=False
        )

        if version in written:
            assert _digest == digest.encode()
            assert (
                config_state_serializer.deserialize(_data) ==
                taa_expected_state_data[version]
            )
        else:
            assert _digest is None
            assert _data is None

    for data in taa_input_data:
        # TODO: Use separate ratification time from txn_time
        taa_handler._add_taa_to_state(data.digest, data.seq_no,
                                      data.txn_time, data.text, data.version, data.txn_time)
        written.append(data.version)

        digest = taa_expected_digests[data.version]
        assert state.get(
            WriteRequestManager._state_path_taa_latest(),
            isCommitted=False
        ) == digest.encode()

        for version in taa_expected_state_data:
            _check_state(version)


def test_get_taa_digest(taa_handler, write_manager, taa_input_data,
        taa_expected_data, taa_expected_digests
):
    """ `get_taa_digest` returns expected value """
    written = []
    for data in taa_input_data:
        taa_handler._add_taa_to_state(data.digest, data.seq_no,
                                      data.txn_time, data.text, data.version, data.txn_time)
        written.append(data.version)

        assert (
            write_manager.get_taa_digest(isCommitted=False) ==
            taa_expected_digests[data.version]
        )

        for version in taa_expected_data:
            digest = write_manager.get_taa_digest(
                version=version, isCommitted=False)
            assert (
                digest ==
                (taa_expected_digests[version] if version in written else None)
            )


def test_get_taa_data(taa_handler, write_manager,
        taa_input_data, taa_expected_data, taa_expected_digests
):
    """ `get_taa_data` returns expected value """
    written = []
    for data in taa_input_data:
        # TODO: Use separate ratification time from txn_time
        taa_handler._add_taa_to_state(data.digest, data.seq_no,
                                      data.txn_time, data.text, data.version, data.txn_time)
        written.append(data.version)

        assert (
            write_manager.get_taa_data(isCommitted=False) ==
            (taa_expected_data[data.version], taa_expected_digests[data.version])
        )

        for version in taa_expected_data:
            expected = (
                (taa_expected_data[version], taa_expected_digests[version])
                if version in written else None
            )
            assert (
                expected ==
                write_manager.get_taa_data(version=version, isCommitted=False)
            )
            assert (
                expected ==
                write_manager.get_taa_data(
                    digest=taa_expected_digests[version],
                    version='any-version-since-ignored',
                    isCommitted=False
                )
            )


def test_update_taa_aml(taa_aml_handler, taa_aml_input_data, taa_handler, write_manager,
        taa_aml_expected_state_data, taa_aml_expected_data):
    """ `update_txn_author_agreement` updates state properly """
    state = write_manager.database_manager.get_state(CONFIG_LEDGER_ID)
    written = []

    def _check_state(version):
        expected_data = taa_aml_expected_data[version]

        _data = state.get(
            WriteRequestManager._state_path_taa_aml_version(version),
            isCommitted=False
        )

        if version in written:
            assert (config_state_serializer.deserialize(_data)[VALUE] ==
                    config_state_serializer.deserialize(expected_data))
        else:
            assert _data is None

    for data in taa_aml_input_data:
        taa_aml_handler._update_txn_author_agreement_acceptance_mechanisms(
            {AML_VERSION: data.version, AML: data.aml, AML_CONTEXT: data.amlContext}, data.seq_no, data.txn_time)
        written.append(data.version)

        data_d = data._asdict()
        assert state.get(
            WriteRequestManager._state_path_taa_aml_latest(),
            isCommitted=False) == encode_state_value({AML_CONTEXT: data_d[AML_CONTEXT],
                                                      AML: data_d[AML],
                                                      AML_VERSION: data_d[AML_VERSION]
                                                      },
                                                     data_d['seq_no'],
                                                     data_d['txn_time'])

        for version in taa_aml_expected_state_data:
            _check_state(version)
