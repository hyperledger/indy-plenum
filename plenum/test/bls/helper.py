from common.serializers.serialization import state_roots_serializer
from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.messages.node_messages import Commit, Prepare, PrePrepare
from plenum.common.util import get_utc_epoch
from plenum.test.helper import sendRandomRequests, waitForSufficientRepliesForRequests


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
        state_roots.append(
            state_roots_serializer.serialize(
                bytes(txnPoolNodeSet[0].getState(DOMAIN_LEDGER_ID).committedHeadHash)))

    # 2. get all saved multi-sigs
    multi_sigs_for_batch = []
    for state_root in state_roots:
        multi_sigs = []
        for node in txnPoolNodeSet:
            multi_sig = node.bls_store.get(state_root)
            if multi_sig:
                multi_sigs.append(multi_sig)
        multi_sigs_for_batch.append(multi_sigs)

    # 3. check how many multi-sigs are saved
    for multi_sigs in multi_sigs_for_batch:
        assert len(multi_sigs) == saved_multi_sigs_count

    # 3. check that bls multi-sig is the same for all nodes we get PrePrepare for (that is for all expect the last one)
    for multi_sigs in multi_sigs_for_batch[:-1]:
        if multi_sigs:
            assert multi_sigs.count(multi_sigs[0]) == len(multi_sigs)


def process_commits_for_key(key, state_root, bls_bfts):
    for sender_bls_bft in bls_bfts:
        commit = create_commit_bls_sig(
            sender_bls_bft,
            key,
            state_root)
        for verifier_bls_bft in bls_bfts:
            verifier_bls_bft.process_commit(commit,
                                            sender_bls_bft.node_id)


def process_ordered(key, bls_bfts, state_root, quorums):
    for bls_bft in bls_bfts:
        bls_bft.process_order(key,
                              state_root,
                              quorums,
                              DOMAIN_LEDGER_ID)


def calculate_multi_sig_for_first(bls_bft_with_commits, quorums, state_root):
    return calculate_multi_sig(bls_bft_with_commits[0],
                               bls_bft_with_commits,
                               quorums,
                               state_root)


def calculate_multi_sig(creator, bls_bft_with_commits, quorums, state_root):
    key = (0, 0)
    for bls_bft_with_commit in bls_bft_with_commits:
        commit = create_commit_bls_sig(
            bls_bft_with_commit,
            key,
            state_root
        )
        creator.process_commit(commit, bls_bft_with_commit.node_id)

    if not creator._can_calculate_multi_sig(key, quorums):
        return None

    return creator._calculate_multi_sig(key)


def create_pre_prepare_params(state_root):
    return [0,
            0,
            0,
            get_utc_epoch(),
            [('1' * 16, 1)],
            0,
            "random digest",
            DOMAIN_LEDGER_ID,
            state_root,
            '1' * 32]


def create_pre_prepare_no_bls_multisig(state_root):
    params = create_pre_prepare_params(state_root)
    return PrePrepare(*params)


def create_pre_prepare_bls_multisig(bls_multi_sig, state_root):
    params = create_pre_prepare_params(state_root)
    params.append([bls_multi_sig.signature, bls_multi_sig.participants, bls_multi_sig.pool_state_root])
    params.append(state_root)
    return PrePrepare(*params)


def create_pre_prepare_bls_multisig_no_state(bls_multi_sig, state_root):
    params = create_pre_prepare_params(state_root)
    params.append([bls_multi_sig.signature, bls_multi_sig.participants, bls_multi_sig.pool_state_root])
    return PrePrepare(*params)


def create_commit_params(view_no, pp_seq_no):
    return [0, view_no, pp_seq_no]


def create_commit_no_bls_sig(req_key):
    view_no, pp_seq_no = req_key
    params = create_commit_params(view_no, pp_seq_no)
    return Commit(*params)


def create_commit_bls_sig(bls_bft, req_key, state_root_hash):
    view_no, pp_seq_no = req_key
    params = create_commit_params(view_no, pp_seq_no)
    params = bls_bft.update_commit(params, state_root_hash, DOMAIN_LEDGER_ID)
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
