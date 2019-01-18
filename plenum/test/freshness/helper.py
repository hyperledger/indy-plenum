from common.serializers.serialization import state_roots_serializer


def check_freshness_updated_for_all(nodes):
    assert all(
        (bls_multi_sig is not None) and (ledger_id == bls_multi_sig.ledger_id)
        for ledger_id, bls_multi_sig in get_all_multi_sig_values_for_all_nodes(nodes)
    )


def check_freshness_updated_for_ledger(nodes, ledger_id):
    assert all(
        (bls_multi_sig is not None) and (ledger_id == bls_multi_sig.ledger_id)
        for bls_multi_sig in get_multi_sig_values_for_all_nodes(nodes, ledger_id)
    )


def get_all_multi_sig_values_for_all_nodes(txnPoolNodeSet):
    result = []
    for node in txnPoolNodeSet:
        for ledger_id, state in node.states.items():
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
