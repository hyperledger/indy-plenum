import os

import base58
import pytest
from plenum.bls.bls import BlsFactoryCharm
from crypto.bls.bls_bft import BlsValidationError
from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.messages.node_messages import \
    PrePrepare, \
    BlsMultiSignature, \
    Prepare
from plenum.common.types import f
from plenum.common.util import get_utc_epoch
from plenum.server.quorums import Quorums
from crypto.bls.bls_bft import BlsBft
from plenum.bls.bls_bft_plenum import BlsBftPlenum
from typing import List

@pytest.fixture()
def bls_bfts(txnPoolNodeSet):
    bls_bfts = []
    for node in txnPoolNodeSet:
        factory = BlsFactoryCharm(node.basedirpath, node.name)
        bls_bft = factory.create_bls_bft()
        bls_bft.bls_key_register.load_latest_keys(node.poolLedger)
        bls_bfts.append(bls_bft)
    return bls_bfts


@pytest.fixture()
def quorums(txnPoolNodeSet):
    return Quorums(len(txnPoolNodeSet))


@pytest.fixture()
def state_root():
    return generate_state_root()


def generate_state_root():
    return base58.b58encode(os.urandom(32))


def test_sign(bls_bfts, state_root):
    for bls_bft in bls_bfts:
        bls_sig = bls_bft.sign_state(state_root)
        assert bls_sig
        assert isinstance(bls_sig, str)


def test_create_multi_sig_from_all(bls_bfts, quorums, state_root):
    multi_sig = calculate_multi_sig(
        creator=bls_bfts[0],
        bls_bfts=bls_bfts,
        quorums=quorums,
        state_root=state_root
    )
    assert multi_sig
    assert isinstance(multi_sig, str)


def test_create_multi_sig_quorum(bls_bfts, quorums, state_root):
    # success on n-f=3
    multi_sig = calculate_multi_sig(
        creator=bls_bfts[0],
        bls_bfts=bls_bfts[1:],
        quorums=quorums,
        state_root=state_root
    )
    assert multi_sig
    assert isinstance(multi_sig, str)


def test_create_multi_sig_no_quorum(bls_bfts, quorums, state_root):
    # not success on 2
    multi_sig = calculate_multi_sig(
        creator=bls_bfts[0],
        bls_bfts=bls_bfts[2:],
        quorums=quorums,
        state_root=state_root
    )
    assert not multi_sig


def test_create_multi_sig_no_quorum_empty(bls_bfts, quorums, state_root):
    multi_sig = calculate_multi_sig(
        creator=bls_bfts[0],
        bls_bfts=[],
        quorums=quorums,
        state_root=state_root
    )
    assert not multi_sig


def test_create_multi_sig_are_equal(bls_bfts, quorums, state_root):
    multi_sigs = set()
    for creator in bls_bfts:
        multi_sig = calculate_multi_sig(
            creator=creator,
            bls_bfts=bls_bfts,
            quorums=quorums,
            state_root=state_root
        )
        multi_sigs.add(multi_sig)

    assert len(multi_sigs) == 1


def test_validate_pre_prepare_no_sigs(bls_bfts, state_root):
    pre_prepare = create_pre_prepare_no_bls_multisig(state_root)
    for sender_bls_bft in bls_bfts:
        for verifier_bls_bft in bls_bfts:
            verifier_bls_bft.validate_pre_prepare(pre_prepare, sender_bls_bft.node_id)


def test_validate_pre_prepare_correct_multi_sig(bls_bfts, state_root, quorums):
    multi_sig_value = calculate_multi_sig(
        creator=bls_bfts[0],
        bls_bfts=bls_bfts,
        quorums=quorums,
        state_root=state_root
    )
    multi_sig_nodes_ids = [bls_bft.node_id for bls_bft in bls_bfts]
    multi_sig = (multi_sig_nodes_ids, multi_sig_value)

    for sender_bls_bft in bls_bfts:
        pre_prepare = create_pre_prepare_bls_multisig(
            bls_multi_sig=multi_sig, state_root=state_root)
        for verifier_bls_bft in bls_bfts:
            verifier_bls_bft.validate_pre_prepare(pre_prepare, sender_bls_bft.node_id)


def test_validate_pre_prepare_incorrect_multi_sig(bls_bfts, state_root, quorums):
    changed_multi_sig_value = calculate_multi_sig(
        creator=bls_bfts[0],
        bls_bfts=bls_bfts,
        quorums=quorums,
        state_root=generate_state_root()
    )
    multi_sig_nodes_ids = [bls_bft.node_id for bls_bft in bls_bfts]
    changed_multi_sig = (multi_sig_nodes_ids, changed_multi_sig_value)

    for sender_bls_bft in bls_bfts:
        pre_prepare = create_pre_prepare_bls_multisig(
            bls_multi_sig=changed_multi_sig, state_root=state_root)
        for verifier_bls_bft in bls_bfts:
            with pytest.raises(BlsValidationError) as ex_info:
                verifier_bls_bft.validate_pre_prepare(pre_prepare, sender_bls_bft.node_id)
            ex_info.match("Multi-sig validation failed")


def test_validate_commit_no_sigs(bls_bfts, state_root):
    key = (0, 0)
    commit = create_prepare_no_bls_sig(key)
    for sender_bls_bft in bls_bfts:
        for verifier_bls_bft in bls_bfts:
            verifier_bls_bft.validate_commit(commit, sender_bls_bft.node_id)


def test_validate_commit_correct_sig(bls_bfts, state_root):
    key = (0, 0)
    for sender_bls_bft in bls_bfts:
        bls_sig = sender_bls_bft.sign_state(state_root)
        commit = create_prepare_bls_sig(key, state_root, bls_sig)
        for verifier_bls_bft in bls_bfts:
            verifier_bls_bft.validate_commit(commit, sender_bls_bft.node_id)


@pytest.mark.skip("Tested functionality is not implemented")
def test_validate_commit_incorrect_sig(bls_bfts, state_root):
    key = (0, 0)
    for sender_bls_bft in bls_bfts:
        bls_sig_changed = sender_bls_bft.sign_state(generate_state_root())
        commit = create_prepare_bls_sig(state_root, bls_sig_changed)
        for verifier_bls_bft in bls_bfts:
            with pytest.raises(BlsValidationError) as ex_info:
                verifier_bls_bft.validate_commit(commit, sender_bls_bft.node_id)
            ex_info.match("Validation failed")


def test_signatures_saved_in_bls(bls_bfts: List[BlsBftPlenum]):
    key1 = (0, 0)
    state1 = generate_state_root()
    validate_prepares_for_key(key1, state1, bls_bfts)
    for bls_bft in bls_bfts:
        assert len(bls_bft._signatures) == 1
        assert len(bls_bft._signatures[key1]) == len(bls_bfts)

    state2 = generate_state_root()
    validate_prepares_for_key(key1, state2, bls_bfts)
    for bls_bft in bls_bfts:
        assert len(bls_bft._signatures) == 1
        assert len(bls_bft._signatures[key1]) == 2 * len(bls_bfts)

    key2 = (0, 1)
    state1 = generate_state_root()
    validate_prepares_for_key(key2, state1, bls_bfts)
    for bls_bft in bls_bfts:
        assert len(bls_bft._signatures) == 2
        assert len(bls_bft._signatures[key1]) == 2 * len(bls_bfts)
        assert len(bls_bft._signatures[key2]) == len(bls_bfts)

    state2 = generate_state_root()
    validate_prepares_for_key(key2, state2, bls_bfts)
    for bls_bft in bls_bfts:
        assert len(bls_bft._signatures) == 2
        assert len(bls_bft._signatures[key1]) == 2 * len(bls_bfts)
        assert len(bls_bft._signatures[key2]) == 2 * len(bls_bfts)

    key3 = (1, 0)
    state1 = generate_state_root()
    validate_prepares_for_key(key3, state1, bls_bfts)
    for bls_bft in bls_bfts:
        assert len(bls_bft._signatures) == 3
        assert len(bls_bft._signatures[key1]) == 2 * len(bls_bfts)
        assert len(bls_bft._signatures[key2]) == 2 * len(bls_bfts)
        assert len(bls_bft._signatures[key3]) == len(bls_bfts)
    state2 = generate_state_root()
    validate_prepares_for_key(key3, state2, bls_bfts)
    for bls_bft in bls_bfts:
        assert len(bls_bft._signatures) == 3
        assert len(bls_bft._signatures[key1]) == 2 * len(bls_bfts)
        assert len(bls_bft._signatures[key2]) == 2 * len(bls_bfts)
        assert len(bls_bft._signatures[key3]) == 2 * len(bls_bfts)


def test_commits_gc(bls_bfts: List[BlsBftPlenum]):
    key1 = (0, 0)
    state1 = generate_state_root()
    validate_prepares_for_key(key1, state1, bls_bfts)

    key2 = (0, 1)
    state1 = generate_state_root()
    validate_prepares_for_key(key2, state1, bls_bfts)

    key3 = (1, 0)
    state1 = generate_state_root()
    validate_prepares_for_key(key3, state1, bls_bfts)

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


def validate_prepares_for_key(key, state_root, bls_bfts):
    for sender_bls_bft in bls_bfts:
        prepare = create_prepare_bls_sig(
            key,
            state_root,
            sender_bls_bft.sign_state(state_root)
        )
        for verifier_bls_bft in bls_bfts:
            verifier_bls_bft.validate_prepare(prepare, sender_bls_bft.node_id)


def calculate_multi_sig(creator: BlsBft, bls_bfts, quorums, state_root):
    key = (0, 0)
    for bls_bft in bls_bfts:
        prepare = create_prepare_bls_sig(
            key,
            state_root,
            bls_bft.sign_state(state_root)
        )
        creator.validate_prepare(prepare, bls_bft.node_id)
    return creator.calculate_multi_sig(key, quorums)


def create_pre_prepare_no_bls_multisig(state_root):
    return PrePrepare(
        0,
        0,
        0,
        get_utc_epoch(),
        [('1' * 16, 1)],
        0,
        "random digest",
        DOMAIN_LEDGER_ID,
        state_root,
        '1' * 32
    )


def create_pre_prepare_bls_multisig(bls_multi_sig, state_root):
    return PrePrepare(
        0,
        0,
        0,
        get_utc_epoch(),
        [('1' * 16, 1)],
        0,
        "random digest",
        DOMAIN_LEDGER_ID,
        state_root,
        '1' * 32,
        bls_multi_sig
    )


def create_prepare_no_bls_sig(key):
    viewNo, ppSeqNo = key
    return Prepare(0, viewNo, ppSeqNo, 1499906902, "123", None, None)


def create_prepare_bls_sig(key, state_root, bls_sig):
    viewNo, ppSeqNo = key
    return Prepare(0, viewNo, ppSeqNo, 1499906902, "123", state_root, None, bls_sig)
