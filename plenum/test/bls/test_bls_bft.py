import os

import base58
import pytest
from plenum.bls.bls import BlsFactoryCharm
from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.exceptions import SuspiciousNode
from plenum.common.messages.node_messages import PrePrepare, BlsMultiSignature, Commit
from plenum.common.types import f
from plenum.common.util import get_utc_epoch
from plenum.server.quorums import Quorums
from plenum.server.suspicion_codes import Suspicions


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
        bls_bft_with_commits=bls_bfts,
        quorums=quorums,
        state_root=state_root
    )
    assert multi_sig
    assert isinstance(multi_sig, str)


def test_create_multi_sig_quorum(bls_bfts, quorums, state_root):
    # success on n-f=3
    multi_sig = calculate_multi_sig(
        creator=bls_bfts[0],
        bls_bft_with_commits=bls_bfts[1:],
        quorums=quorums,
        state_root=state_root
    )
    assert multi_sig
    assert isinstance(multi_sig, str)

    # not success on 2
    multi_sig = calculate_multi_sig(
        creator=bls_bfts[0],
        bls_bft_with_commits=bls_bfts[2:],
        quorums=quorums,
        state_root=state_root
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
    multi_sigs = set()
    for creator in bls_bfts:
        multi_sig = calculate_multi_sig(
            creator=creator,
            bls_bft_with_commits=bls_bfts,
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
        bls_bft_with_commits=bls_bfts,
        quorums=quorums,
        state_root=state_root
    )
    multi_sig_nodes_ids = [bls_bft.node_id for bls_bft in bls_bfts]
    multi_sig = {f.BLS_MULTI_SIG_NODES.nm: multi_sig_nodes_ids,
                 f.BLS_MULTI_SIG_VALUE.nm: multi_sig_value}

    for sender_bls_bft in bls_bfts:
        pre_prepare = create_pre_prepare_bls_multisig(
            bls_multi_sig=multi_sig, state_root=state_root)
        for verifier_bls_bft in bls_bfts:
            verifier_bls_bft.validate_pre_prepare(pre_prepare, sender_bls_bft.node_id)


def test_validate_pre_prepare_incorrect_multi_sig(bls_bfts, state_root, quorums):
    changed_multi_sig_value = calculate_multi_sig(
        creator=bls_bfts[0],
        bls_bft_with_commits=bls_bfts,
        quorums=quorums,
        state_root=generate_state_root()
    )
    multi_sig_nodes_ids = [bls_bft.node_id for bls_bft in bls_bfts]
    changed_multi_sig = {f.BLS_MULTI_SIG_NODES.nm: multi_sig_nodes_ids,
                         f.BLS_MULTI_SIG_VALUE.nm: changed_multi_sig_value}

    for sender_bls_bft in bls_bfts:
        pre_prepare = create_pre_prepare_bls_multisig(
            bls_multi_sig=changed_multi_sig, state_root=state_root)
        for verifier_bls_bft in bls_bfts:
            with pytest.raises(SuspiciousNode) as ex_info:
                verifier_bls_bft.validate_pre_prepare(pre_prepare, sender_bls_bft.node_id)
            ex_info.match(Suspicions.PPR_BLS_MULTISIG_WRONG.reason)


def test_validate_commit_no_sigs(bls_bfts, state_root):
    commit = create_commit_no_bls_sig()
    for sender_bls_bft in bls_bfts:
        for verifier_bls_bft in bls_bfts:
            verifier_bls_bft.validate_commit(commit, sender_bls_bft.node_id, state_root)


def test_validate_commit_correct_sig(bls_bfts, state_root):
    for sender_bls_bft in bls_bfts:
        bls_sig = sender_bls_bft.sign_state(state_root)
        commit = create_commit_bls_sig(bls_sig)
        for verifier_bls_bft in bls_bfts:
            verifier_bls_bft.validate_commit(commit, sender_bls_bft.node_id, state_root)


def test_validate_commit_incorrect_sig(bls_bfts, state_root):
    for sender_bls_bft in bls_bfts:
        bls_sig_changed = sender_bls_bft.sign_state(generate_state_root())
        commit = create_commit_bls_sig(bls_sig_changed)
        for verifier_bls_bft in bls_bfts:
            with pytest.raises(SuspiciousNode) as ex_info:
                verifier_bls_bft.validate_commit(commit, sender_bls_bft.node_id, state_root)
            ex_info.match(Suspicions.CM_BLS_SIG_WRONG.reason)


def calculate_multi_sig(creator, bls_bft_with_commits, quorums, state_root):
    commits = []
    for bls_bft_with_commit in bls_bft_with_commits:
        commits.append(
            create_commit_bls_sig(
                bls_bft_with_commit.sign_state(state_root)
            )
        )

    return creator.calculate_multi_sig(
        (0, 0), quorums, commits)


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


def create_pre_prepare_bls_multisig(bls_multi_sig: BlsMultiSignature, state_root):
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


def create_commit_no_bls_sig():
    return Commit(0, 0, 0)


def create_commit_bls_sig(bls_sig):
    return Commit(0, 0, 0, bls_sig)
