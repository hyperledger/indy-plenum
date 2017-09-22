from common.serializers.serialization import state_roots_serializer
from plenum.common.constants import DOMAIN_LEDGER_ID
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