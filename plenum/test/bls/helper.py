import base58
import os

from common.serializers.serialization import state_roots_serializer
from plenum.common.constants import DOMAIN_LEDGER_ID, ALIAS, BLS_KEY
from plenum.common.keygen_utils import init_bls_keys
from plenum.common.messages.node_messages import Commit, Prepare, PrePrepare
from plenum.common.util import get_utc_epoch, randomString, random_from_alphabet
from plenum.test.helper import sendRandomRequests, waitForSufficientRepliesForRequests
from plenum.test.node_catchup.helper import waitNodeDataEquality, ensureClientConnectedToNodesAndPoolLedgerSame
from plenum.test.pool_transactions.helper import updateNodeData, new_client


def generate_state_root():
    return base58.b58encode(os.urandom(32))


def check_bls_multi_sig_after_send(looper, txnPoolNodeSet,
                                   client, wallet,
                                   saved_multi_sigs_count):
    # at least two because first request could have no
    # signature since state can be clear
    number_of_requests = 3

    # 1. send requests
    # Using loop to avoid 3pc batching
    state_roots = []
    for i in range(number_of_requests):
        reqs = sendRandomRequests(wallet, client, 1)
        waitForSufficientRepliesForRequests(looper, client, requests=reqs)
        waitNodeDataEquality(looper, txnPoolNodeSet[0], *txnPoolNodeSet[:-1])
        state_roots.append(
            state_roots_serializer.serialize(
                bytes(txnPoolNodeSet[0].getState(DOMAIN_LEDGER_ID).committedHeadHash)))

    # 2. get all saved multi-sigs
    multi_sigs_for_batch = []
    for state_root in state_roots:
        multi_sigs = []
        for node in txnPoolNodeSet:
            multi_sig = node.bls_bft.bls_store.get(state_root)
            if multi_sig:
                multi_sigs.append(multi_sig)
        multi_sigs_for_batch.append(multi_sigs)

    # 3. check how many multi-sigs are saved
    for multi_sigs in multi_sigs_for_batch:
        assert len(multi_sigs) == saved_multi_sigs_count,\
            "{} != {}".format(len(multi_sigs), saved_multi_sigs_count)

    # 3. check that bls multi-sig is the same for all nodes we get PrePrepare for (that is for all expect the last one)
    for multi_sigs in multi_sigs_for_batch[:-1]:
        if multi_sigs:
            assert multi_sigs.count(multi_sigs[0]) == len(multi_sigs)


def process_commits_for_key(key, pre_prepare, bls_bfts):
    for sender_bls_bft in bls_bfts:
        commit = create_commit_bls_sig(
            sender_bls_bft,
            key,
            pre_prepare)
        for verifier_bls_bft in bls_bfts:
            verifier_bls_bft.process_commit(commit,
                                            sender_bls_bft.node_id)


def process_ordered(key, bls_bfts, pre_prepare, quorums):
    for bls_bft in bls_bfts:
        bls_bft.process_order(key,
                              quorums,
                              pre_prepare)


def calculate_multi_sig(creator, bls_bft_with_commits, quorums, pre_prepare):
    key = (0, 0)
    for bls_bft_with_commit in bls_bft_with_commits:
        commit = create_commit_bls_sig(
            bls_bft_with_commit,
            key,
            pre_prepare
        )
        creator.process_commit(commit, bls_bft_with_commit.node_id)

    if not creator._can_calculate_multi_sig(key, quorums):
        return None

    return creator._calculate_multi_sig(key, pre_prepare)


def create_pre_prepare_params(state_root,
                              ledger_id = DOMAIN_LEDGER_ID,
                              txn_root=None,
                              timestamp=None,
                              bls_multi_sig=None):
    params= [0,
            0,
            0,
            timestamp or get_utc_epoch(),
            [('1' * 16, 1)],
            0,
            "random digest",
            ledger_id,
            state_root,
            txn_root or '1' * 32]
    if bls_multi_sig:
        params.append(bls_multi_sig.as_list())
    return params


def create_pre_prepare_no_bls(state_root):
    params = create_pre_prepare_params(state_root=state_root)
    return PrePrepare(*params)


def create_commit_params(view_no, pp_seq_no):
    return [0, view_no, pp_seq_no]


def create_commit_no_bls_sig(req_key):
    view_no, pp_seq_no = req_key
    params = create_commit_params(view_no, pp_seq_no)
    return Commit(*params)

def create_commit_with_bls_sig(req_key, bls_sig):
    view_no, pp_seq_no = req_key
    params = create_commit_params(view_no, pp_seq_no)
    params.append(bls_sig)
    return Commit(*params)

def create_commit_bls_sig(bls_bft, req_key, pre_prepare):
    view_no, pp_seq_no = req_key
    params = create_commit_params(view_no, pp_seq_no)
    params = bls_bft.update_commit(params, pre_prepare)
    return Commit(*params)


def create_prepare_params(view_no, pp_seq_no, state_root):
    return [0,
            view_no,
            pp_seq_no,
            get_utc_epoch(),
            "random digest",
            state_root,
            '1' * 32]


def create_prepare(req_key, state_root):
    view_no, pp_seq_no = req_key
    params = create_prepare_params(view_no, pp_seq_no, state_root)
    return Prepare(*params)


def change_bls_key(looper, txnPoolNodeSet,
                   node,
                   steward_client, steward_wallet,
                   add_wrong=False):
    new_blspk = init_bls_keys(node.keys_dir, node.name)

    key_in_txn = \
        new_blspk \
        if not add_wrong \
        else ''.join(random_from_alphabet(32, base58.alphabet))

    node_data = {
        ALIAS: node.name,
        BLS_KEY: key_in_txn
    }

    updateNodeData(looper, steward_client, steward_wallet, node, node_data)
    waitNodeDataEquality(looper, node, *txnPoolNodeSet[:-1])
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, steward_client,
                                                  *txnPoolNodeSet)
    return new_blspk


def check_bls_key(blskey, node, nodes, add_wrong=False):
    '''
    Check that each node has the same and correct blskey for this node
    '''
    keys = set()
    for n in nodes:
        keys.add(n.bls_bft.bls_key_register.get_key_by_name(node.name))
    assert len(keys) == 1
    if not add_wrong:
        assert blskey == next(iter(keys))

    # check that this node has correct blskey
    if not add_wrong:
        assert node.bls_bft.can_sign_bls()
        assert blskey == node.bls_bft.bls_crypto_signer.pk
    else:
        assert not node.bls_bft.can_sign_bls()


def check_update_bls_key(node_num, saved_multi_sigs_count,
                         looper, txnPoolNodeSet,
                         client_tdir,
                         poolTxnClientData,
                         stewards_and_wallets,
                         add_wrong=False):
    # 1. Change BLS key for a specified NODE
    node = txnPoolNodeSet[node_num]
    steward_client, steward_wallet = stewards_and_wallets[node_num]
    new_blspk = change_bls_key(looper, txnPoolNodeSet, node,
                               steward_client, steward_wallet,
                               add_wrong)

    # 2. Check that all Nodes see the new BLS key value
    check_bls_key(new_blspk, node, txnPoolNodeSet, add_wrong)

    # 3. Check that we can send new requests and have correct multisigs
    client, wallet = new_client(looper,
                                poolTxnClientData,
                                txnPoolNodeSet, client_tdir)
    check_bls_multi_sig_after_send(looper, txnPoolNodeSet,
                                   client, wallet,
                                   saved_multi_sigs_count=saved_multi_sigs_count)
