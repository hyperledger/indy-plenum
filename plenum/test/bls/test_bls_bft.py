import os
from copy import copy

import base58
import pytest
from crypto.bls.bls_multi_signature import MultiSignature
from plenum.bls.bls_bft_factory import create_default_bls_bft_factory
from plenum.common.constants import DOMAIN_LEDGER_ID, POOL_LEDGER_ID
from plenum.common.exceptions import SuspiciousNode
from plenum.server.quorums import Quorums
from plenum.server.suspicion_codes import Suspicions
from plenum.test.bls.helper import process_commits_for_key, calculate_multi_sig, create_commit_no_bls_sig, \
    create_pre_prepare_bls_multisig_no_state, create_prepare, create_pre_prepare_no_bls_multisig, create_commit_bls_sig, \
    create_pre_prepare_bls_multisig, create_commit_params, create_pre_prepare_params, process_ordered, \
    create_prepare_params, calculate_multi_sig_for_first


@pytest.fixture()
def bls_bfts(txnPoolNodeSet):
    bls_bfts = []
    for node in txnPoolNodeSet:
        bls_bft = create_default_bls_bft_factory(node).create_bls_bft(is_master=True)
        bls_bfts.append(bls_bft)
    return bls_bfts


@pytest.fixture()
def quorums(txnPoolNodeSet):
    return Quorums(len(txnPoolNodeSet))


@pytest.fixture()
def state_root():
    return generate_state_root()


@pytest.fixture()
def pool_state_root():
    return generate_state_root()


def generate_state_root():
    return base58.b58encode(os.urandom(32))


# ------ CREATE 3PC MESSAGES ------

def test_update_pre_prepare_first_time(bls_bfts, state_root):
    params = create_pre_prepare_params(state_root)
    params_initial = copy(params)
    for bls_bft in bls_bfts:
        params = bls_bft.update_pre_prepare(params, DOMAIN_LEDGER_ID)
        assert params == params_initial


def test_update_pre_prepare_after_ordered(bls_bfts, state_root, quorums):
    key = (0, 0)
    process_commits_for_key(key, state_root, bls_bfts)
    process_ordered(key, bls_bfts, state_root, quorums)
    params = create_pre_prepare_params(state_root)
    params_initial = copy(params)
    for bls_bft in bls_bfts:
        params = bls_bft.update_pre_prepare(params, DOMAIN_LEDGER_ID)
        assert params != params_initial


def test_update_pre_prepare_after_ordered_pool_ledger(bls_bfts, state_root):
    params = create_pre_prepare_params(state_root)
    params_initial = copy(params)
    for bls_bft in bls_bfts:
        params = bls_bft.update_pre_prepare(params, POOL_LEDGER_ID)
        assert params == params_initial


def test_update_prepare(bls_bfts, state_root):
    params = create_prepare_params(0, 0, state_root)
    params_initial = copy(params)
    for bls_bft in bls_bfts:
        params = bls_bft.update_prepare(params, DOMAIN_LEDGER_ID)
        assert params == params_initial


def test_update_commit(bls_bfts, state_root):
    params = create_commit_params(0, 0)
    params_initial = copy(params)
    for bls_bft in bls_bfts:
        params = bls_bft.update_commit(params, state_root, DOMAIN_LEDGER_ID)
        assert params != params_initial


def test_update_commit_pool_ledger(bls_bfts, state_root):
    params = create_commit_params(0, 0)
    params_initial = copy(params)
    for bls_bft in bls_bfts:
        params = bls_bft.update_commit(params, state_root, POOL_LEDGER_ID)
        assert params == params_initial


# ------ VALIDATE 3PC MESSAGES ------

def test_validate_pre_prepare_no_sigs(bls_bfts, state_root):
    pre_prepare = create_pre_prepare_no_bls_multisig(state_root)
    for sender_bls_bft in bls_bfts:
        for verifier_bls_bft in bls_bfts:
            verifier_bls_bft.validate_pre_prepare(pre_prepare, sender_bls_bft.node_id)


def test_validate_pre_prepare_correct_multi_sig(bls_bfts, state_root, quorums):
    multi_sig = calculate_multi_sig_for_first(bls_bfts, quorums, state_root)
    for sender_bls_bft in bls_bfts:
        pre_prepare = create_pre_prepare_bls_multisig(
            bls_multi_sig=multi_sig, state_root=state_root)
        for verifier_bls_bft in bls_bfts:
            verifier_bls_bft.validate_pre_prepare(pre_prepare, sender_bls_bft.node_id)


def test_validate_pre_prepare_multi_sig_no_state(bls_bfts, state_root, quorums):
    multi_sig = calculate_multi_sig_for_first(bls_bfts, quorums, state_root)
    for sender_bls_bft in bls_bfts:
        pre_prepare = create_pre_prepare_bls_multisig_no_state(
            bls_multi_sig=multi_sig, state_root=state_root)
        for verifier_bls_bft in bls_bfts:
            with pytest.raises(SuspiciousNode) as ex_info:
                verifier_bls_bft.validate_pre_prepare(pre_prepare, sender_bls_bft.node_id)
            ex_info.match(Suspicions.PPR_NO_BLS_MULTISIG_STATE.reason)


def test_validate_pre_prepare_incorrect_multi_sig(bls_bfts, state_root, quorums):
    changed_root = generate_state_root()
    changed_multi_sig = calculate_multi_sig_for_first(bls_bfts, quorums, changed_root)
    for sender_bls_bft in bls_bfts:
        pre_prepare = create_pre_prepare_bls_multisig(
            bls_multi_sig=changed_multi_sig, state_root=state_root)
        for verifier_bls_bft in bls_bfts:
            with pytest.raises(SuspiciousNode) as ex_info:
                verifier_bls_bft.validate_pre_prepare(pre_prepare, sender_bls_bft.node_id)
            ex_info.match(Suspicions.PPR_BLS_MULTISIG_WRONG.reason)


def test_validate_pre_prepare_incorrect_multi_sig_no_state(bls_bfts, state_root, quorums):
    changed_root = generate_state_root()
    changed_multi_sig = calculate_multi_sig_for_first(bls_bfts, quorums, changed_root)
    for sender_bls_bft in bls_bfts:
        pre_prepare = create_pre_prepare_bls_multisig_no_state(
            bls_multi_sig=changed_multi_sig, state_root=state_root)
        for verifier_bls_bft in bls_bfts:
            with pytest.raises(SuspiciousNode) as ex_info:
                verifier_bls_bft.validate_pre_prepare(pre_prepare, sender_bls_bft.node_id)
            ex_info.match(Suspicions.PPR_NO_BLS_MULTISIG_STATE.reason)


def test_validate_prepare(bls_bfts, state_root):
    prepare = create_prepare((0, 0), state_root)
    for sender_bls_bft in bls_bfts:
        for verifier_bls_bft in bls_bfts:
            verifier_bls_bft.validate_prepare(prepare, sender_bls_bft.node_id)


def test_validate_commit_no_sigs(bls_bfts, state_root):
    key = (0, 0)
    commit = create_commit_no_bls_sig(key)
    for sender_bls_bft in bls_bfts:
        for verifier_bls_bft in bls_bfts:
            verifier_bls_bft.validate_commit(commit,
                                             sender_bls_bft.node_id,
                                             state_root)


def test_validate_commit_correct_sig(bls_bfts, state_root):
    key = (0, 0)
    for sender_bls_bft in bls_bfts:
        commit = create_commit_bls_sig(sender_bls_bft, key, state_root)
        for verifier_bls_bft in bls_bfts:
            verifier_bls_bft.validate_commit(commit,
                                             sender_bls_bft.node_id,
                                             state_root)


def test_validate_commit_incorrect_sig(bls_bfts, state_root):
    key = (0, 0)
    for sender_bls_bft in bls_bfts:
        commit = create_commit_bls_sig(sender_bls_bft, key, generate_state_root())
        for verifier_bls_bft in bls_bfts:
            with pytest.raises(SuspiciousNode) as ex_info:
                verifier_bls_bft.validate_commit(commit,
                                                 sender_bls_bft.node_id,
                                                 state_root)
            ex_info.match(Suspicions.CM_BLS_SIG_WRONG.reason)


# ------ PROCESS 3PC MESSAGES ------

def test_process_pre_prepare_no_multisig(bls_bfts, state_root):
    pre_prepare = create_pre_prepare_no_bls_multisig(state_root)
    for sender_bls_bft in bls_bfts:
        for verifier_bls_bft in bls_bfts:
            verifier_bls_bft.process_pre_prepare(pre_prepare, sender_bls_bft.node_id)


def test_process_pre_prepare_multisig(bls_bfts, state_root, quorums):
    multi_sig = calculate_multi_sig_for_first(bls_bfts, quorums, state_root)
    for sender_bls_bft in bls_bfts:
        pre_prepare = create_pre_prepare_bls_multisig(
            bls_multi_sig=multi_sig, state_root=state_root)
        for verifier_bls_bft in bls_bfts:
            verifier_bls_bft.process_pre_prepare(pre_prepare, sender_bls_bft.node_id)


def test_process_prepare(bls_bfts, state_root):
    for sender_bls_bft in bls_bfts:
        prepare = create_prepare((0, 0), state_root)
        for verifier_bls_bft in bls_bfts:
            verifier_bls_bft.process_prepare(prepare, sender_bls_bft.node_id)


def test_process_commit_no_sigs(bls_bfts):
    for sender_bls_bft in bls_bfts:
        commit = create_commit_no_bls_sig((0, 0))
        for verifier_bls_bft in bls_bfts:
            verifier_bls_bft.process_commit(commit,
                                            sender_bls_bft.node_id)


def test_process_commit_with_sigs(bls_bfts, state_root):
    for sender_bls_bft in bls_bfts:
        commit = create_commit_bls_sig(
            sender_bls_bft,
            (0, 0),
            state_root)
        for verifier_bls_bft in bls_bfts:
            verifier_bls_bft.process_commit(commit,
                                            sender_bls_bft.node_id)


def test_process_order(bls_bfts, state_root, quorums):
    key = (0, 0)
    process_commits_for_key(key, state_root, bls_bfts)
    for bls_bft in bls_bfts:
        bls_bft.process_order(key,
                              state_root,
                              quorums,
                              DOMAIN_LEDGER_ID)


# ------ CREATE MULTI_SIG ------

def test_create_multi_sig_from_all(bls_bfts, quorums, state_root):
    multi_sig = calculate_multi_sig(
        creator=bls_bfts[0],
        bls_bft_with_commits=bls_bfts,
        quorums=quorums,
        state_root=state_root
    )
    assert multi_sig
    assert isinstance(multi_sig, MultiSignature)


def test_create_multi_sig_quorum(bls_bfts, quorums, state_root):
    # success on n-f=3
    multi_sig = calculate_multi_sig(
        creator=bls_bfts[0],
        bls_bft_with_commits=bls_bfts[1:],
        quorums=quorums,
        state_root=state_root
    )
    assert multi_sig
    assert isinstance(multi_sig, MultiSignature)


def test_create_multi_sig_no_quorum(bls_bfts, quorums, state_root):
    # not success on 2
    multi_sig = calculate_multi_sig(
        creator=bls_bfts[0],
        bls_bft_with_commits=bls_bfts[2:],
        quorums=quorums,
        state_root=state_root,
    )
    assert not multi_sig


def test_create_multi_sig_no_quorum_empty(bls_bfts, quorums, state_root):
    multi_sig = calculate_multi_sig(
        creator=bls_bfts[0],
        bls_bft_with_commits=[],
        quorums=quorums,
        state_root=state_root
    )
    assert not multi_sig


def test_create_multi_sig_are_equal(bls_bfts, quorums, state_root):
    multi_sigs = []
    for creator in bls_bfts:
        multi_sig = calculate_multi_sig(
            creator=creator,
            bls_bft_with_commits=bls_bfts,
            quorums=quorums,
            state_root=state_root
        )
        multi_sigs.append(multi_sig)

    assert all(x == multi_sigs[0] for x in multi_sigs)


# ------ MULTI_SIG SAVED ------

def test_signatures_cached_for_commits(bls_bfts):
    key1 = (0, 0)
    state1 = generate_state_root()
    process_commits_for_key(key1, state1, bls_bfts)
    for bls_bft in bls_bfts:
        assert len(bls_bft._signatures) == 1
        assert len(bls_bft._signatures[key1]) == len(bls_bfts)

    state2 = generate_state_root()
    process_commits_for_key(key1, state2, bls_bfts)
    for bls_bft in bls_bfts:
        assert len(bls_bft._signatures) == 1
        assert len(bls_bft._signatures[key1]) == len(bls_bfts)

    key2 = (0, 1)
    state1 = generate_state_root()
    process_commits_for_key(key2, state1, bls_bfts)
    for bls_bft in bls_bfts:
        assert len(bls_bft._signatures) == 2
        assert len(bls_bft._signatures[key1]) == len(bls_bfts)
        assert len(bls_bft._signatures[key2]) == len(bls_bfts)

    state2 = generate_state_root()
    process_commits_for_key(key2, state2, bls_bfts)
    for bls_bft in bls_bfts:
        assert len(bls_bft._signatures) == 2
        assert len(bls_bft._signatures[key1]) == len(bls_bfts)
        assert len(bls_bft._signatures[key2]) == len(bls_bfts)

    key3 = (1, 0)
    state1 = generate_state_root()
    process_commits_for_key(key3, state1, bls_bfts)
    for bls_bft in bls_bfts:
        assert len(bls_bft._signatures) == 3
        assert len(bls_bft._signatures[key1]) == len(bls_bfts)
        assert len(bls_bft._signatures[key2]) == len(bls_bfts)
        assert len(bls_bft._signatures[key3]) == len(bls_bfts)
    state2 = generate_state_root()
    process_commits_for_key(key3, state2, bls_bfts)
    for bls_bft in bls_bfts:
        assert len(bls_bft._signatures) == 3
        assert len(bls_bft._signatures[key1]) == len(bls_bfts)
        assert len(bls_bft._signatures[key2]) == len(bls_bfts)
        assert len(bls_bft._signatures[key3]) == len(bls_bfts)


def test_multi_sig_saved_locally_for_ordered(bls_bfts, state_root, quorums):
    key = (0, 0)
    process_commits_for_key(key, state_root, bls_bfts)
    process_ordered(key, bls_bfts, state_root, quorums)
    for bls_bft in bls_bfts:
        assert bls_bft._bls_store.get(state_root)


def test_multi_sig_saved_shared_with_pre_prepare(bls_bfts, quorums, state_root):
    multi_sig = calculate_multi_sig_for_first(bls_bfts, quorums, state_root)
    pre_prepare = create_pre_prepare_bls_multisig(
        bls_multi_sig=multi_sig, state_root=state_root)

    multi_sigs = []
    for bls_bft in bls_bfts:
        bls_bft.process_pre_prepare(pre_prepare, bls_bfts[0].node_id)
        multi_sig = bls_bft._bls_store.get(state_root)
        assert multi_sig
        multi_sigs.append(multi_sig)

    # all saved multi-sigs are equal
    assert all(x == multi_sigs[0] for x in multi_sigs)


def test_preprepare_multisig_replaces_saved(bls_bfts, quorums, state_root):
    # have locally calculated multi-sigs
    key = (0, 0)
    for sender_bls_bft in bls_bfts:
        commit = create_commit_bls_sig(
            sender_bls_bft,
            key,
            state_root)
        for verifier_bls_bft in bls_bfts:
            # use 3 of 4 commits only
            if verifier_bls_bft != sender_bls_bft:
                verifier_bls_bft.process_commit(commit,
                                                sender_bls_bft.node_id)
    process_ordered(key, bls_bfts, state_root, quorums)

    # get locally calculated multi-sigs
    local_multi_sigs = {}
    for bls_bft in bls_bfts:
        local_multi_sigs[bls_bft.node_id] = bls_bft._bls_store.get(state_root)

    # have multi-sig for PrePrepare (make it different from the local one by using al 4 nodes)
    multi_sig = calculate_multi_sig_for_first(bls_bfts, quorums, state_root)
    pre_prepare = create_pre_prepare_bls_multisig(
        bls_multi_sig=multi_sig, state_root=state_root)

    # get multi-sigs get with PrePrepare and make sure they differ from local ones
    # the local ones must be overridden
    multi_sigs = []
    for bls_bft in bls_bfts:
        bls_bft.process_pre_prepare(pre_prepare, bls_bfts[0].node_id)
        multi_sig = bls_bft._bls_store.get(state_root)
        local_multi_sig = local_multi_sigs[bls_bft.node_id]
        assert multi_sig
        assert local_multi_sig
        assert multi_sig != local_multi_sig
        multi_sigs.append(multi_sig)

    # all saved multi-sigs are equal
    assert all(x == multi_sigs[0] for x in multi_sigs)


# ------ GC ------

def test_commits_gc(bls_bfts):
    key1 = (0, 0)
    state1 = generate_state_root()
    process_commits_for_key(key1, state1, bls_bfts)

    key2 = (0, 1)
    state1 = generate_state_root()
    process_commits_for_key(key2, state1, bls_bfts)

    key3 = (1, 0)
    state1 = generate_state_root()
    process_commits_for_key(key3, state1, bls_bfts)

    for bls_bft in bls_bfts:
        assert len(bls_bft._signatures) == 3
        assert key1 in bls_bft._signatures
        assert key2 in bls_bft._signatures
        assert key3 in bls_bft._signatures

    for bls_bft in bls_bfts:
        bls_bft.gc((0, 1))

    for bls_bft in bls_bfts:
        assert len(bls_bft._signatures) == 1
        assert not key1 in bls_bft._signatures
        assert not key2 in bls_bft._signatures
        assert len(bls_bft._signatures[key3]) == len(bls_bfts)
