import os

import base58
import pytest
from crypto.bls.bls_multi_signature import MultiSignature
from plenum.bls.bls import BlsFactoryIndyCrypto
from plenum.common.constants import DOMAIN_LEDGER_ID, POOL_LEDGER_ID
from plenum.common.exceptions import SuspiciousNode
from plenum.common.messages.node_messages import PrePrepare, Commit
from plenum.common.util import get_utc_epoch
from plenum.server.quorums import Quorums
from plenum.server.suspicion_codes import Suspicions


@pytest.fixture()
def bls_bfts(txnPoolNodeSet):
    bls_bfts = []
    for node in txnPoolNodeSet:
        factory = BlsFactoryIndyCrypto(basedir=node.basedirpath,
                                       node_name=node.name,
                                       data_location=node.dataLocation,
                                       config=node.config)
        bls_bft = factory.create_bls_bft(is_master=True,
                                         pool_state=node.getState(POOL_LEDGER_ID),
                                         bls_store=node.bls_store)
        bls_bft.bls_key_register.load_latest_keys(node.poolLedger)
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


def test_create_multi_sig_from_all(bls_bfts, quorums, state_root, pool_state_root):
    multi_sig = calculate_multi_sig(
        creator=bls_bfts[0],
        bls_bft_with_commits=bls_bfts,
        quorums=quorums,
        state_root=state_root,
        pool_state_root=pool_state_root
    )
    assert multi_sig
    assert isinstance(multi_sig, MultiSignature)


def test_create_multi_sig_quorum(bls_bfts, quorums, state_root, pool_state_root):
    # success on n-f=3
    multi_sig = calculate_multi_sig(
        creator=bls_bfts[0],
        bls_bft_with_commits=bls_bfts[1:],
        quorums=quorums,
        state_root=state_root,
        pool_state_root=pool_state_root
    )
    assert multi_sig
    assert isinstance(multi_sig, MultiSignature)


def test_create_multi_sig_no_quorum(bls_bfts, quorums, state_root, pool_state_root):
    # not success on 2
    multi_sig = calculate_multi_sig(
        creator=bls_bfts[0],
        bls_bft_with_commits=bls_bfts[2:],
        quorums=quorums,
        state_root=state_root,
        pool_state_root=pool_state_root
    )
    assert not multi_sig


def test_create_multi_sig_no_quorum_empty(bls_bfts, quorums, state_root, pool_state_root):
    multi_sig = calculate_multi_sig(
        creator=bls_bfts[0],
        bls_bft_with_commits=[],
        quorums=quorums,
        state_root=state_root,
        pool_state_root=pool_state_root
    )
    assert not multi_sig


def test_create_multi_sig_are_equal(bls_bfts, quorums, state_root, pool_state_root):
    multi_sigs = []
    for creator in bls_bfts:
        multi_sig = calculate_multi_sig(
            creator=creator,
            bls_bft_with_commits=bls_bfts,
            quorums=quorums,
            state_root=state_root,
            pool_state_root=pool_state_root
        )
        multi_sigs.append(multi_sig)

    assert all(x == multi_sigs[0] for x in multi_sigs)


def test_validate_pre_prepare_no_sigs(bls_bfts, state_root):
    pre_prepare = create_pre_prepare_no_bls_multisig(state_root)
    for sender_bls_bft in bls_bfts:
        for verifier_bls_bft in bls_bfts:
            verifier_bls_bft.validate_pre_prepare(pre_prepare, sender_bls_bft.node_id)


def test_validate_pre_prepare_correct_multi_sig(bls_bfts, state_root, pool_state_root, quorums):
    multi_sig = calculate_multi_sig(
        creator=bls_bfts[0],
        bls_bft_with_commits=bls_bfts,
        quorums=quorums,
        state_root=state_root,
        pool_state_root=pool_state_root
    )

    for sender_bls_bft in bls_bfts:
        pre_prepare = create_pre_prepare_bls_multisig(
            bls_multi_sig=multi_sig, state_root=state_root)
        for verifier_bls_bft in bls_bfts:
            verifier_bls_bft.validate_pre_prepare(pre_prepare, sender_bls_bft.node_id)


def test_validate_pre_prepare_incorrect_multi_sig(bls_bfts, state_root, pool_state_root, quorums):
    changed_root = generate_state_root()
    changed_multi_sig = calculate_multi_sig(
        creator=bls_bfts[0],
        bls_bft_with_commits=bls_bfts,
        quorums=quorums,
        state_root=changed_root,
        pool_state_root=pool_state_root
    )

    for sender_bls_bft in bls_bfts:
        pre_prepare = create_pre_prepare_bls_multisig(
            bls_multi_sig=changed_multi_sig, state_root=state_root)
        for verifier_bls_bft in bls_bfts:
            with pytest.raises(SuspiciousNode) as ex_info:
                verifier_bls_bft.validate_pre_prepare(pre_prepare, sender_bls_bft.node_id)
            ex_info.match(Suspicions.PPR_BLS_MULTISIG_WRONG.reason)


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


def test_signatures_saved(bls_bfts):
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


def process_commits_for_key(key, state_root, bls_bfts):
    for sender_bls_bft in bls_bfts:
        commit = create_commit_bls_sig(
            sender_bls_bft,
            key,
            state_root)
        for verifier_bls_bft in bls_bfts:
            verifier_bls_bft.process_commit(commit,
                                            sender_bls_bft.node_id)


def calculate_multi_sig(creator, bls_bft_with_commits, quorums, state_root, pool_state_root):
    key = (0, 0)
    for bls_bft_with_commit in bls_bft_with_commits:
        commit = create_commit_bls_sig(
            bls_bft_with_commit,
            key,
            state_root
        )
        creator.process_commit(commit, bls_bft_with_commit.node_id)

    return creator._calculate_multi_sig(key, quorums, pool_state_root)


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
        (bls_multi_sig.signature, bls_multi_sig.participants, bls_multi_sig.pool_state_root),
        state_root
    )


def create_commit_no_bls_sig(req_key):
    view_no, pp_seq_no = req_key
    return Commit(0, view_no, pp_seq_no)


def create_commit_bls_sig(bls_bft, req_key, state_root_hash):
    view_no, pp_seq_no = req_key
    params = [0, view_no, pp_seq_no]
    params = bls_bft.update_commit(params, state_root_hash, DOMAIN_LEDGER_ID)
    return Commit(*params)
