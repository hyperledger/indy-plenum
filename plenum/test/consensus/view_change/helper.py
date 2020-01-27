from functools import partial
from typing import Optional, List

import base58

from plenum.common.messages.internal_messages import NewViewCheckpointsApplied
from plenum.common.messages.node_messages import PrePrepare, Checkpoint
from plenum.server.consensus.view_change_service import ViewChangeService
from plenum.server.consensus.batch_id import BatchID
from plenum.test.consensus.helper import SimPool
from plenum.test.simulation.sim_random import SimRandom


def some_checkpoint(random: SimRandom, view_no: int, pp_seq_no: int) -> Checkpoint:
    return Checkpoint(
        instId=0, viewNo=view_no, seqNoStart=pp_seq_no, seqNoEnd=pp_seq_no,
        digest=base58.b58encode(random.string(32)).decode())


def some_pool(random: SimRandom) -> (SimPool, List):
    pool_size = random.integer(4, 8)
    pool = SimPool(pool_size, random)
    view_no = pool._initial_view_no
    log_size = pool.nodes[0].config.LOG_SIZE

    # Create simulated history
    # TODO: Move into helper?
    faulty = (pool_size - 1) // 3
    seq_no_per_cp = 10
    max_batches = 50
    batches = [BatchID(view_no, view_no, n, random.string(40)) for n in range(1, max_batches)]
    checkpoints = [some_checkpoint(random, view_no, n) for n in range(0, max_batches, seq_no_per_cp)]

    # Preprepares
    pp_count = [random.integer(0, len(batches)) for _ in range(pool_size)]
    max_pp = sorted(pp_count)[faulty]
    # Prepares
    p_count = [random.integer(0, min(max_pp, pp)) for pp in pp_count]
    max_p = sorted(p_count)[faulty]
    # Checkpoints
    cp_count = [1 + random.integer(0, min(max_p, p)) // seq_no_per_cp for p in pp_count]
    max_stable_cp_indx = sorted(cp_count)[faulty]
    stable_cp = [checkpoints[random.integer(0, min(max_stable_cp_indx, cp) - 1)].seqNoEnd for cp in cp_count]

    # Initialize consensus data
    for i, node in enumerate(pool.nodes):
        high_watermark = stable_cp[i] + log_size
        node._data.preprepared = batches[:min(high_watermark, pp_count[i])]
        node._data.prepared = batches[:min(high_watermark, p_count[i])]
        node._data.checkpoints.clear()
        node._data.checkpoints.update(checkpoints[:cp_count[i]])
        node._data.stable_checkpoint = stable_cp[i]

    # Mock Ordering service to update preprepares for new view
    for node in pool.nodes:
        def update_shared_data(node, msg: NewViewCheckpointsApplied):
            x = [
                BatchID(view_no=msg.view_no, pp_view_no=batch_id.pp_view_no, pp_seq_no=batch_id.pp_seq_no,
                        pp_digest=batch_id.pp_digest)
                for batch_id in msg.batches
            ]
            node._orderer._data.preprepared = x

        node._orderer._subscription.subscribe(node._orderer._stasher, NewViewCheckpointsApplied, partial(update_shared_data, node))

    committed = []
    for i in range(1, max_batches):
        prepare_count = sum(1 for node in pool.nodes if i <= len(node._data.prepared))
        has_prepared_cert = prepare_count >= pool_size - faulty
        if has_prepared_cert:
            batch_id = batches[i - 1]
            committed.append(BatchID(batch_id.view_no + 1, batch_id.pp_view_no, batch_id.pp_seq_no, batch_id.pp_digest))

    return pool, committed


def calc_committed(view_changes, max_pp_seq_no, n, f) -> List[BatchID]:
    def check_prepared_in_vc(vc, batch_id):
        # check that batch_id is present in VC's prepared and preprepared
        for p_batch_id in vc.prepared:
            if batch_id != p_batch_id:
                continue
            for pp_batch_id in vc.preprepared:
                if batch_id == pp_batch_id:
                    return True

        return False

    def find_batch_id(pp_seq_no):
        for vc in view_changes:
            for batch_id in vc.prepared:
                if batch_id[2] != pp_seq_no:
                    continue
                prepared_count = sum(1 for vc in view_changes if check_prepared_in_vc(vc, batch_id))
                if prepared_count < n - f:
                    continue
                return batch_id
        return None

    committed = []
    for pp_seq_no in range(1, max_pp_seq_no):
        batch_id = find_batch_id(pp_seq_no)
        if batch_id is not None:
            committed.append(BatchID(*batch_id))

    return committed
