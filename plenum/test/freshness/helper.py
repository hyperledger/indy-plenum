from common.serializers.serialization import state_roots_serializer
from stp_core.loop.eventually import eventually


def check_freshness_updated_for_all(nodes):
    for ledger_id, bls_multi_sig in get_all_multi_sig_values_for_all_nodes(nodes):
        assert bls_multi_sig is not None, \
            "bls_multi_sig is None for ledger {}".format(ledger_id)
        assert ledger_id == bls_multi_sig.ledger_id, \
            "expected ledger {}, got {}".format(ledger_id, bls_multi_sig.ledger_id)


def check_freshness_updated_for_ledger(nodes, ledger_id):
    for bls_multi_sig in get_multi_sig_values_for_all_nodes(nodes, ledger_id):
        assert bls_multi_sig is not None, \
            "bls_multi_sig is None for ledger {}".format(ledger_id)
        assert ledger_id == bls_multi_sig.ledger_id, \
            "expected ledger {}, got {}".format(ledger_id, bls_multi_sig.ledger_id)


def get_all_multi_sig_values_for_all_nodes(txnPoolNodeSet):
    result = []
    for node in txnPoolNodeSet:
        for ledger_id, state in node.states.items():
            # TODO: Is it expected that in case of identical states (for example both empty)
            # this can mix ledgers up?
            bls_multi_sig = node.bls_bft.bls_store.get(
                state_roots_serializer.serialize(bytes(state.committedHeadHash))
            )
            result.append((ledger_id, bls_multi_sig.value) if bls_multi_sig else (ledger_id, None))
    return result


def get_multi_sig_values_for_all_nodes(txnPoolNodeSet, ledger_id):
    result = []
    for node in txnPoolNodeSet:
        bls_multi_sig = node.bls_bft.bls_store.get(
            state_roots_serializer.serialize(bytes(node.states[ledger_id].committedHeadHash))
        )
        result.append(bls_multi_sig.value if bls_multi_sig else None)
    return result


def check_updated_bls_multi_sig_for_all_ledgers(nodes, previous_multi_sigs, freshness_timeout):
    multi_sigs = get_all_multi_sig_values_for_all_nodes(nodes)
    for ((ledger_id1, bls_multi_sig), (ledger_id2, previous_bls_multi_sig)) in zip(multi_sigs, previous_multi_sigs):
        assert ledger_id1 == ledger_id2
        check_updated_bls_multisig_values(ledger_id1, bls_multi_sig, previous_bls_multi_sig, freshness_timeout)


def check_updated_bls_multi_sig_for_ledger(nodes, ledger_id, previous_multi_sigs, freshness_timeout):
    multi_sigs = get_multi_sig_values_for_all_nodes(nodes, ledger_id)
    for (bls_multi_sig, previous_bls_multi_sig) in zip(multi_sigs, previous_multi_sigs):
        check_updated_bls_multisig_values(ledger_id, bls_multi_sig, previous_bls_multi_sig, freshness_timeout)


def check_updated_bls_multisig_values(ledger_id, new_value, old_value, freshness_timeout):
    assert new_value is not None, \
        "{}: value is None {}".format(ledger_id, new_value)
    assert new_value.timestamp - old_value.timestamp >= freshness_timeout, \
        "{}: freshness delta is {}".format(ledger_id, new_value.timestamp - old_value.timestamp)
    assert new_value.state_root_hash == old_value.state_root_hash, \
        "{}: new state root {}, old state root {}".format(ledger_id, new_value.state_root_hash,
                                                          old_value.state_root_hash)
    assert new_value.txn_root_hash == old_value.txn_root_hash, \
        "{}: new txn root {}, txn state root {}".format(ledger_id, new_value.txn_root_hash, old_value.txn_root_hash)
    assert new_value.ledger_id == ledger_id, \
        "{}: ledger id {} != expected ledger id {}".format(ledger_id, new_value.ledger_id, ledger_id)


def check_update_bls_multi_sig_during_ordering(looper, txnPoolNodeSet,
                                               send_txn_func,
                                               freshness_timeout,
                                               ordered_ledger_id, refreshed_ledger_id):
    # 1. Wait for first freshness update
    looper.run(eventually(
        check_freshness_updated_for_all, txnPoolNodeSet,
        timeout=freshness_timeout + 5)
    )
    refreshed_bls_multi_sigs_after_first_update = get_multi_sig_values_for_all_nodes(txnPoolNodeSet,
                                                                                     refreshed_ledger_id)

    # 2. order txns
    looper.runFor(3)  # delay update
    send_txn_func()

    # 3. Wait for the second freshness update.
    #  It's expected for the ledger we don't have any ordered requests only
    ordered_bls_multi_sigs_after_txns_sent = get_multi_sig_values_for_all_nodes(txnPoolNodeSet, ordered_ledger_id)
    looper.run(eventually(check_updated_bls_multi_sig_for_ledger,
                          txnPoolNodeSet, refreshed_ledger_id,
                          refreshed_bls_multi_sigs_after_first_update,
                          freshness_timeout,
                          timeout=freshness_timeout + 5
                          ))
    ordered_bls_multi_sigs_after_refreshed_update = get_multi_sig_values_for_all_nodes(txnPoolNodeSet,
                                                                                       ordered_ledger_id)
    assert ordered_bls_multi_sigs_after_txns_sent == ordered_bls_multi_sigs_after_refreshed_update

    # 4. Third freshness update should update the ledger where we had ordered requests,
    # while it's too early to update the second ledger yet
    refreshed_bls_multi_sigs_after_refreshed_update = get_multi_sig_values_for_all_nodes(txnPoolNodeSet,
                                                                                         refreshed_ledger_id)
    looper.run(eventually(check_updated_bls_multi_sig_for_ledger,
                          txnPoolNodeSet, ordered_ledger_id,
                          ordered_bls_multi_sigs_after_refreshed_update,
                          freshness_timeout,
                          timeout=freshness_timeout + 5
                          ))
    assert refreshed_bls_multi_sigs_after_refreshed_update == get_multi_sig_values_for_all_nodes(txnPoolNodeSet,
                                                                                                 refreshed_ledger_id)


def has_freshness_instance_change(node, count=1):
    all_instance_changes = node.master_replica._view_change_trigger_service.spylog.getAll('_send_instance_change')
    freshness_instance_changes = sum(1 for ic in all_instance_changes
                                     if ic.params['suspicion'].code == 43)
    return freshness_instance_changes >= count
