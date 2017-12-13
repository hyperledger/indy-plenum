from copy import copy

import base58
import pytest

from crypto.bls.bls_bft_replica import BlsBftReplica
from crypto.bls.bls_multi_signature import MultiSignature, MultiSignatureValue
from plenum.bls.bls_bft_factory import create_default_bls_bft_factory
from plenum.common.constants import DOMAIN_LEDGER_ID, POOL_LEDGER_ID
from plenum.common.messages.node_messages import PrePrepare
from plenum.common.util import get_utc_epoch
from plenum.server.quorums import Quorums
from plenum.test.bls.helper import process_commits_for_key, calculate_multi_sig, create_commit_no_bls_sig, \
    create_prepare, create_commit_bls_sig, \
    create_commit_params, create_pre_prepare_params, process_ordered, \
    create_prepare_params, generate_state_root, create_commit_with_bls_sig, \
    create_pre_prepare_no_bls

whitelist = ['Indy Crypto error']

@pytest.fixture()
def bls_bft_replicas(txnPoolNodeSet):
    bls_bft_replicas = []
    for node in txnPoolNodeSet:
        bls_bft_replica = create_default_bls_bft_factory(node).create_bls_bft_replica(is_master=True)
        bls_bft_replicas.append(bls_bft_replica)
    return bls_bft_replicas


@pytest.fixture()
def quorums(txnPoolNodeSet):
    return Quorums(len(txnPoolNodeSet))


@pytest.fixture()
def state_root():
    return generate_state_root()


@pytest.fixture()
def pool_state_root(bls_bft_replicas):
    bls_bft_replica = bls_bft_replicas[0]
    return bls_bft_replica.state_root_serializer.serialize(
        bytes(bls_bft_replica._bls_bft.bls_key_register.get_pool_root_hash_committed()))


@pytest.fixture()
def fake_pre_prepare_with_bls(fake_multi_sig):
    params = create_pre_prepare_params(state_root=fake_multi_sig.value.state_root_hash,
                                       bls_multi_sig=fake_multi_sig)
    return PrePrepare(*params)


@pytest.fixture()
def fake_pre_prepare_with_bls_pool_ledger(fake_multi_sig):
    params = create_pre_prepare_params(state_root=fake_multi_sig.value.state_root_hash,
                                       bls_multi_sig=fake_multi_sig,
                                       ledger_id=POOL_LEDGER_ID)
    return PrePrepare(*params)


@pytest.fixture()
def multi_sig_value(state_root, pool_state_root):
    return MultiSignatureValue(ledger_id=DOMAIN_LEDGER_ID,
                               state_root_hash=state_root,
                               pool_state_root_hash=pool_state_root,
                               txn_root_hash=generate_state_root(),
                               timestamp=get_utc_epoch())


@pytest.fixture()
def multi_signature(bls_bft_replicas, multi_sig_value):
    sigs = []
    participants = []
    message = multi_sig_value.as_single_value()
    for bls_bft_replica in bls_bft_replicas:
        sigs.append(bls_bft_replica._bls_bft.bls_crypto_signer.sign(message))
        participants.append(bls_bft_replica.node_id)

    multi_sig = bls_bft_replicas[0]._bls_bft.bls_crypto_verifier.create_multi_sig(sigs)

    return MultiSignature(signature=multi_sig,
                          participants=participants,
                          value=multi_sig_value)


@pytest.fixture()
def pre_prepare_with_bls(multi_signature):
    params = create_pre_prepare_params(state_root=multi_signature.value.state_root_hash,
                                       bls_multi_sig=multi_signature)
    return PrePrepare(*params)


@pytest.fixture()
def pre_prepare_with_bls_pool_ledger(multi_signature):
    params = create_pre_prepare_params(state_root=multi_signature.value.state_root_hash,
                                       bls_multi_sig=multi_signature,
                                       ledger_id=POOL_LEDGER_ID)
    return PrePrepare(*params)


@pytest.fixture()
def pre_prepare_with_incorrect_bls(multi_signature):
    multi_signature.signature = base58.b58encode(b"somefakesignaturesomefakesignaturesomefakesignature")
    params = create_pre_prepare_params(state_root=multi_signature.value.state_root_hash,
                                       bls_multi_sig=multi_signature)
    return PrePrepare(*params)


@pytest.fixture()
def pre_prepare_with_incorrect_bls_pool_ledger(multi_signature):
    multi_signature.signature = base58.b58encode(b"somefakesignaturesomefakesignaturesomefakesignature")
    params = create_pre_prepare_params(state_root=multi_signature.value.state_root_hash,
                                       bls_multi_sig=multi_signature,
                                       ledger_id=POOL_LEDGER_ID)
    return PrePrepare(*params)


@pytest.fixture()
def pre_prepare_no_bls(state_root):
    params = create_pre_prepare_params(state_root=state_root)
    return PrePrepare(*params)


@pytest.yield_fixture(scope="function", params=['state_root', 'timestamp', 'txn_root'])
def pre_prepare_incorrect(state_root, request):
    if request.param == 'state_root':
        params = create_pre_prepare_params(state_root=generate_state_root())
    elif request.param == 'ledger_id':
        params = create_pre_prepare_params(state_root=state_root, ledger_id=DOMAIN_LEDGER_ID)
    elif request.param == 'timestamp':
        params = create_pre_prepare_params(state_root=state_root, timestamp=get_utc_epoch() + 1000)
    elif request.param == 'txn_root':
        params = create_pre_prepare_params(state_root=state_root, txn_root=generate_state_root())
    return PrePrepare(*params)


# ------ CREATE 3PC MESSAGES ------

def test_update_pre_prepare_first_time(bls_bft_replicas, state_root):
    params = create_pre_prepare_params(state_root)
    params_initial = copy(params)
    for bls_bft_replica in bls_bft_replicas:
        params = bls_bft_replica.update_pre_prepare(params, DOMAIN_LEDGER_ID)
        assert params == params_initial


def test_update_pre_prepare_after_ordered(bls_bft_replicas, state_root, fake_multi_sig):
    for bls_bft_replica in bls_bft_replicas:
        bls_bft_replica._bls_latest_multi_sig = fake_multi_sig

    params = create_pre_prepare_params(state_root)

    params_initial = copy(params)
    for bls_bft_replica in bls_bft_replicas:
        params = bls_bft_replica.update_pre_prepare(params, DOMAIN_LEDGER_ID)
        assert params != params_initial


def test_update_pre_prepare_after_ordered_pool_ledger(bls_bft_replicas,
                                                      state_root, fake_multi_sig):
    for bls_bft_replica in bls_bft_replicas:
        bls_bft_replica._bls_latest_multi_sig = fake_multi_sig

    params = create_pre_prepare_params(state_root)

    params_initial = copy(params)
    for bls_bft_replica in bls_bft_replicas:
        params = bls_bft_replica.update_pre_prepare(params, POOL_LEDGER_ID)
        assert params == params_initial


def test_update_prepare(bls_bft_replicas, state_root):
    params = create_prepare_params(0, 0, state_root)
    params_initial = copy(params)
    for bls_bft_replica in bls_bft_replicas:
        params = bls_bft_replica.update_prepare(params, DOMAIN_LEDGER_ID)
        assert params == params_initial


def test_update_commit(bls_bft_replicas, fake_pre_prepare_with_bls):
    params = create_commit_params(0, 0)
    params_initial = copy(params)
    for bls_bft_replica in bls_bft_replicas:
        params = bls_bft_replica.update_commit(params, fake_pre_prepare_with_bls)
        assert params != params_initial


def test_update_commit_pool_ledger(bls_bft_replicas, fake_pre_prepare_with_bls_pool_ledger):
    params = create_commit_params(0, 0)
    params_initial = copy(params)
    for bls_bft_replica in bls_bft_replicas:
        params = bls_bft_replica.update_commit(params,
                                               fake_pre_prepare_with_bls_pool_ledger)
        assert params == params_initial


# ------ VALIDATE 3PC MESSAGES ------

def test_validate_pre_prepare_no_sigs(bls_bft_replicas, pre_prepare_no_bls):
    for sender_bls_bft_replica in bls_bft_replicas:
        for verifier_bls_bft_replica in bls_bft_replicas:
            assert not verifier_bls_bft_replica.validate_pre_prepare(pre_prepare_no_bls,
                                                                     sender_bls_bft_replica.node_id)


def test_validate_pre_prepare_correct_multi_sig(bls_bft_replicas, pre_prepare_with_bls):
    for sender_bls_bft_replica in bls_bft_replicas:
        for verifier_bls_bft_replica in bls_bft_replicas:
            assert not verifier_bls_bft_replica.validate_pre_prepare(pre_prepare_with_bls,
                                                                     sender_bls_bft_replica.node_id)


def test_validate_pre_prepare_correct_multi_sig_pool(bls_bft_replicas,
                                                     pre_prepare_with_bls_pool_ledger):
    for sender_bls_bft_replica in bls_bft_replicas:
        for verifier_bls_bft_replica in bls_bft_replicas:
            assert not verifier_bls_bft_replica.validate_pre_prepare(pre_prepare_with_bls_pool_ledger,
                                                                     sender_bls_bft_replica.node_id)


def test_validate_pre_prepare_incorrect_multi_sig(bls_bft_replicas,
                                                  pre_prepare_with_incorrect_bls):
    for sender_bls_bft in bls_bft_replicas:
        for verifier_bls_bft in bls_bft_replicas:
            status = verifier_bls_bft.validate_pre_prepare(pre_prepare_with_incorrect_bls,
                                                           sender_bls_bft.node_id)
            assert status == BlsBftReplica.PPR_BLS_MULTISIG_WRONG


def test_validate_pre_prepare_incorrect_multi_sig_pool(bls_bft_replicas,
                                                       pre_prepare_with_incorrect_bls_pool_ledger):
    for sender_bls_bft in bls_bft_replicas:
        for verifier_bls_bft in bls_bft_replicas:
            status = verifier_bls_bft.validate_pre_prepare(pre_prepare_with_incorrect_bls_pool_ledger,
                                                           sender_bls_bft.node_id)
            assert status == BlsBftReplica.PPR_BLS_MULTISIG_WRONG


def test_validate_prepare(bls_bft_replicas, state_root):
    prepare = create_prepare((0, 0), state_root)
    for sender_bls_bft in bls_bft_replicas:
        for verifier_bls_bft in bls_bft_replicas:
            assert not verifier_bls_bft.validate_prepare(prepare, sender_bls_bft.node_id)


def test_validate_commit_no_sigs(bls_bft_replicas):
    key = (0, 0)
    commit = create_commit_no_bls_sig(key)
    for sender_bls_bft in bls_bft_replicas:
        for verifier_bls_bft in bls_bft_replicas:
            assert not verifier_bls_bft.validate_commit(commit,
                                                        sender_bls_bft.node_id,
                                                        state_root)


def test_validate_commit_correct_sig_first_time(bls_bft_replicas, pre_prepare_no_bls):
    key = (0, 0)
    for sender_bls_bft in bls_bft_replicas:
        commit = create_commit_bls_sig(sender_bls_bft, key, pre_prepare_no_bls)
        for verifier_bls_bft in bls_bft_replicas:
            assert not verifier_bls_bft.validate_commit(commit,
                                                        sender_bls_bft.node_id,
                                                        pre_prepare_no_bls)


def test_validate_commit_correct_sig_second_time(bls_bft_replicas, pre_prepare_with_bls):
    key = (0, 0)
    for sender_bls_bft in bls_bft_replicas:
        commit = create_commit_bls_sig(sender_bls_bft, key, pre_prepare_with_bls)
        for verifier_bls_bft in bls_bft_replicas:
            assert not verifier_bls_bft.validate_commit(commit,
                                                        sender_bls_bft.node_id,
                                                        pre_prepare_with_bls)


def test_validate_commit_incorrect_sig(bls_bft_replicas, pre_prepare_with_bls):
    key = (0, 0)
    for sender_bls_bft in bls_bft_replicas:
        fake_sig = base58.b58encode(b"somefakesignaturesomefakesignaturesomefakesignature")
        commit = create_commit_with_bls_sig(key, fake_sig)
        for verifier_bls_bft in bls_bft_replicas:
            status = verifier_bls_bft.validate_commit(commit,
                                                      sender_bls_bft.node_id,
                                                      pre_prepare_with_bls)
            assert status == BlsBftReplica.CM_BLS_SIG_WRONG


def test_validate_commit_incorrect_sig_pool(bls_bft_replicas, pre_prepare_with_bls_pool_ledger):
    key = (0, 0)
    for sender_bls_bft in bls_bft_replicas:
        fake_sig = base58.b58encode(b"somefakesignaturesomefakesignaturesomefakesignature")
        commit = create_commit_with_bls_sig(key, fake_sig)
        for verifier_bls_bft in bls_bft_replicas:
            status = verifier_bls_bft.validate_commit(commit,
                                                      sender_bls_bft.node_id,
                                                      pre_prepare_with_bls_pool_ledger)
            assert status == BlsBftReplica.CM_BLS_SIG_WRONG


def test_validate_commit_incorrect_value(bls_bft_replicas, pre_prepare_incorrect, pre_prepare_no_bls):
    key = (0, 0)
    for sender_bls_bft in bls_bft_replicas:
        commit = create_commit_bls_sig(sender_bls_bft, key, pre_prepare_incorrect)
        for verifier_bls_bft in bls_bft_replicas:
            status = verifier_bls_bft.validate_commit(commit,
                                                      sender_bls_bft.node_id,
                                                      pre_prepare_no_bls)
            assert status == BlsBftReplica.CM_BLS_SIG_WRONG


def test_validate_commit_incorrect_value_ledger_id(bls_bft_replicas, pre_prepare_no_bls,
                                                   pre_prepare_with_bls_pool_ledger):
    key = (0, 0)
    for sender_bls_bft in bls_bft_replicas:
        commit = create_commit_bls_sig(sender_bls_bft, key, pre_prepare_no_bls)
        for verifier_bls_bft in bls_bft_replicas:
            status = verifier_bls_bft.validate_commit(commit,
                                                      sender_bls_bft.node_id,
                                                      pre_prepare_with_bls_pool_ledger)
            assert status == BlsBftReplica.CM_BLS_SIG_WRONG


# ------ PROCESS 3PC MESSAGES ------

def test_process_pre_prepare_no_multisig(bls_bft_replicas, pre_prepare_no_bls):
    for sender_bls_bft in bls_bft_replicas:
        for verifier_bls_bft in bls_bft_replicas:
            verifier_bls_bft.process_pre_prepare(pre_prepare_no_bls, sender_bls_bft.node_id)


def test_process_pre_prepare_multisig(bls_bft_replicas, pre_prepare_with_bls):
    for sender_bls_bft in bls_bft_replicas:
        for verifier_bls_bft in bls_bft_replicas:
            verifier_bls_bft.process_pre_prepare(pre_prepare_with_bls, sender_bls_bft.node_id)


def test_process_prepare(bls_bft_replicas, state_root):
    for sender_bls_bft in bls_bft_replicas:
        prepare = create_prepare((0, 0), state_root)
        for verifier_bls_bft in bls_bft_replicas:
            verifier_bls_bft.process_prepare(prepare, sender_bls_bft.node_id)


def test_process_commit_no_sigs(bls_bft_replicas):
    for sender_bls_bft in bls_bft_replicas:
        commit = create_commit_no_bls_sig((0, 0))
        for verifier_bls_bft in bls_bft_replicas:
            verifier_bls_bft.process_commit(commit,
                                            sender_bls_bft.node_id)


def test_process_commit_with_sigs(bls_bft_replicas, pre_prepare_no_bls):
    key = (0, 0)
    for sender_bls_bft in bls_bft_replicas:
        commit = create_commit_bls_sig(sender_bls_bft, key, pre_prepare_no_bls)
        for verifier_bls_bft in bls_bft_replicas:
            verifier_bls_bft.process_commit(commit,
                                            sender_bls_bft.node_id)


def test_process_order(bls_bft_replicas, pre_prepare_no_bls, quorums):
    key = (0, 0)
    process_commits_for_key(key, pre_prepare_no_bls, bls_bft_replicas)
    for bls_bft in bls_bft_replicas:
        bls_bft.process_order(key,
                              quorums,
                              pre_prepare_no_bls)


# ------ CREATE MULTI_SIG ------

def test_create_multi_sig_from_all(bls_bft_replicas, quorums, pre_prepare_no_bls):
    multi_sig = calculate_multi_sig(
        creator=bls_bft_replicas[0],
        bls_bft_with_commits=bls_bft_replicas,
        quorums=quorums,
        pre_prepare=pre_prepare_no_bls
    )
    assert multi_sig
    assert isinstance(multi_sig, MultiSignature)


def test_create_multi_sig_quorum(bls_bft_replicas, quorums, pre_prepare_no_bls):
    # success on n-f=3
    multi_sig = calculate_multi_sig(
        creator=bls_bft_replicas[0],
        bls_bft_with_commits=bls_bft_replicas[1:],
        quorums=quorums,
        pre_prepare=pre_prepare_no_bls
    )
    assert multi_sig
    assert isinstance(multi_sig, MultiSignature)


def test_create_multi_sig_no_quorum(bls_bft_replicas, quorums, pre_prepare_no_bls):
    # not success on 2
    multi_sig = calculate_multi_sig(
        creator=bls_bft_replicas[0],
        bls_bft_with_commits=bls_bft_replicas[2:],
        quorums=quorums,
        pre_prepare=pre_prepare_no_bls,
    )
    assert not multi_sig


def test_create_multi_sig_no_quorum_empty(bls_bft_replicas, quorums, pre_prepare_no_bls):
    multi_sig = calculate_multi_sig(
        creator=bls_bft_replicas[0],
        bls_bft_with_commits=[],
        quorums=quorums,
        pre_prepare=pre_prepare_no_bls
    )
    assert not multi_sig


def test_create_multi_sig_are_equal(bls_bft_replicas, quorums, pre_prepare_no_bls):
    multi_sigs = []
    for creator in bls_bft_replicas:
        multi_sig = calculate_multi_sig(
            creator=creator,
            bls_bft_with_commits=bls_bft_replicas,
            quorums=quorums,
            pre_prepare=pre_prepare_no_bls
        )
        multi_sigs.append(multi_sig)

    assert all(x == multi_sigs[0] for x in multi_sigs)


# ------ MULTI_SIG SAVED ------

def test_signatures_cached_for_commits(bls_bft_replicas):
    key1 = (0, 0)
    pre_prepare1 = create_pre_prepare_no_bls(generate_state_root())
    process_commits_for_key(key1, pre_prepare1, bls_bft_replicas)
    for bls_bft in bls_bft_replicas:
        assert len(bls_bft._signatures) == 1
        assert len(bls_bft._signatures[key1]) == len(bls_bft_replicas)

    pre_prepare2 = create_pre_prepare_no_bls(generate_state_root())
    process_commits_for_key(key1, pre_prepare2, bls_bft_replicas)
    for bls_bft in bls_bft_replicas:
        assert len(bls_bft._signatures) == 1
        assert len(bls_bft._signatures[key1]) == len(bls_bft_replicas)

    key2 = (0, 1)
    pre_prepare3 = create_pre_prepare_no_bls(generate_state_root())
    process_commits_for_key(key2, pre_prepare3, bls_bft_replicas)
    for bls_bft in bls_bft_replicas:
        assert len(bls_bft._signatures) == 2
        assert len(bls_bft._signatures[key1]) == len(bls_bft_replicas)
        assert len(bls_bft._signatures[key2]) == len(bls_bft_replicas)

    pre_prepare4 = create_pre_prepare_no_bls(generate_state_root())
    process_commits_for_key(key2, pre_prepare4, bls_bft_replicas)
    for bls_bft in bls_bft_replicas:
        assert len(bls_bft._signatures) == 2
        assert len(bls_bft._signatures[key1]) == len(bls_bft_replicas)
        assert len(bls_bft._signatures[key2]) == len(bls_bft_replicas)

    key3 = (1, 0)
    pre_prepare5 = create_pre_prepare_no_bls(generate_state_root())
    process_commits_for_key(key3, pre_prepare5, bls_bft_replicas)
    for bls_bft in bls_bft_replicas:
        assert len(bls_bft._signatures) == 3
        assert len(bls_bft._signatures[key1]) == len(bls_bft_replicas)
        assert len(bls_bft._signatures[key2]) == len(bls_bft_replicas)
        assert len(bls_bft._signatures[key3]) == len(bls_bft_replicas)

    pre_prepare6 = create_pre_prepare_no_bls(generate_state_root())
    process_commits_for_key(key3, pre_prepare6, bls_bft_replicas)
    for bls_bft in bls_bft_replicas:
        assert len(bls_bft._signatures) == 3
        assert len(bls_bft._signatures[key1]) == len(bls_bft_replicas)
        assert len(bls_bft._signatures[key2]) == len(bls_bft_replicas)
        assert len(bls_bft._signatures[key3]) == len(bls_bft_replicas)


def test_multi_sig_saved_locally_for_ordered(bls_bft_replicas, pre_prepare_no_bls,
                                             state_root, quorums):
    key = (0, 0)
    process_commits_for_key(key, pre_prepare_no_bls, bls_bft_replicas)
    process_ordered(key, bls_bft_replicas, pre_prepare_no_bls, quorums)
    for bls_bft_replica in bls_bft_replicas:
        assert bls_bft_replica._bls_bft.bls_store.get(state_root)


def test_multi_sig_saved_shared_with_pre_prepare(bls_bft_replicas, quorums, pre_prepare_with_bls):
    multi_sigs = []
    for bls_bft_replica in bls_bft_replicas:
        bls_bft_replica.process_pre_prepare(pre_prepare_with_bls, bls_bft_replicas[0].node_id)
        multi_sig = bls_bft_replica._bls_bft.bls_store.get(pre_prepare_with_bls.stateRootHash)
        assert multi_sig
        multi_sigs.append(multi_sig)

    # all saved multi-sigs are equal
    assert all(x == multi_sigs[0] for x in multi_sigs)


def test_preprepare_multisig_replaces_saved(bls_bft_replicas, quorums,
                                            pre_prepare_no_bls, pre_prepare_with_bls):
    # have locally calculated multi-sigs
    key = (0, 0)
    state_root = pre_prepare_no_bls.stateRootHash
    for sender_bls_bft_replica in bls_bft_replicas:
        commit = create_commit_bls_sig(
            sender_bls_bft_replica,
            key,
            pre_prepare_no_bls)
        for verifier_bls_bft_replica in bls_bft_replicas:
            # use 3 of 4 commits only
            if verifier_bls_bft_replica != sender_bls_bft_replica:
                verifier_bls_bft_replica.process_commit(commit,
                                                        sender_bls_bft_replica.node_id)
    process_ordered(key, bls_bft_replicas, pre_prepare_no_bls, quorums)

    # get locally calculated multi-sigs
    local_multi_sigs = {}
    for bls_bft_replica in bls_bft_replicas:
        local_multi_sigs[bls_bft_replica.node_id] = bls_bft_replica._bls_bft.bls_store.get(state_root)

    # have multi-sig for PrePrepare (make it different from the local one by using al 4 nodes)
    # get multi-sigs get with PrePrepare and make sure they differ from local ones
    # the local ones must be overridden
    multi_sigs = []
    for bls_bft_replica in bls_bft_replicas:
        bls_bft_replica.process_pre_prepare(pre_prepare_with_bls, bls_bft_replicas[0].node_id)
        multi_sig = bls_bft_replica._bls_bft.bls_store.get(state_root)
        local_multi_sig = local_multi_sigs[bls_bft_replica.node_id]
        assert multi_sig
        assert local_multi_sig
        assert multi_sig != local_multi_sig
        multi_sigs.append(multi_sig)

    # all saved multi-sigs are equal
    assert all(x == multi_sigs[0] for x in multi_sigs)


# ------ GC ------

def test_commits_gc(bls_bft_replicas):
    key1 = (0, 0)
    pre_prepare1 = create_pre_prepare_no_bls(generate_state_root())
    process_commits_for_key(key1, pre_prepare1, bls_bft_replicas)

    key2 = (0, 1)
    pre_prepare2 = create_pre_prepare_no_bls(generate_state_root())
    process_commits_for_key(key2, pre_prepare2, bls_bft_replicas)

    key3 = (1, 0)
    pre_prepare3 = create_pre_prepare_no_bls(generate_state_root())
    process_commits_for_key(key3, pre_prepare3, bls_bft_replicas)

    for bls_bft in bls_bft_replicas:
        assert len(bls_bft._signatures) == 3
        assert key1 in bls_bft._signatures
        assert key2 in bls_bft._signatures
        assert key3 in bls_bft._signatures

    for bls_bft in bls_bft_replicas:
        bls_bft.gc((0, 1))

    for bls_bft in bls_bft_replicas:
        assert len(bls_bft._signatures) == 1
        assert not key1 in bls_bft._signatures
        assert not key2 in bls_bft._signatures
        assert len(bls_bft._signatures[key3]) == len(bls_bft_replicas)
