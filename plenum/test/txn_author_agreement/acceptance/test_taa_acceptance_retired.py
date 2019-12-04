import pytest

from plenum.test.txn_author_agreement.helper import sdk_send_txn_author_agreement


@pytest.mark.skip()
def test_taa_acceptance_retired(
        tconf, txnPoolNodeSet, validate_taa_acceptance, validation_error,
        turn_off_freshness_state_update, max_last_accepted_pre_prepare_time,
        request_dict, latest_taa, monkeypatch,
        looper, sdk_pool_handle, sdk_wallet_trustee
):
    validate_taa_acceptance(request_dict)
    sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_trustee,
                                  latest_taa.text, latest_taa.version, retired=True)
    with pytest.raises(
            validation_error,
            match=("Txn Author Agreement is retired: version {}".format(latest_taa.version))
    ):
        validate_taa_acceptance(request_dict)

    sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_trustee,
                                  latest_taa.text, latest_taa.version)
    validate_taa_acceptance(request_dict)
