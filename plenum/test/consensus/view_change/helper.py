from typing import Optional, List

from plenum.common.event_bus import InternalBus
from plenum.common.messages.node_messages import PrePrepare, Checkpoint
from plenum.server.consensus.replica_service import ReplicaService
from plenum.server.consensus.view_change_service import ViewChangeService, BatchID
from plenum.test.greek import genNodeNames
from plenum.test.helper import MockTimer
from plenum.test.simulation.sim_network import SimNetwork
from plenum.test.simulation.sim_random import SimRandom, DefaultSimRandom
from plenum.test.testing_utils import FakeSomething


class SimPool:
    def __init__(self, node_count: int = 4, random: Optional[SimRandom] = None):
        self._random = random if random else DefaultSimRandom()
        self._timer = MockTimer()
        self._network = SimNetwork(self._timer, self._random)
        validators = genNodeNames(node_count)
        primary_name = validators[0]
        self._nodes = [ReplicaService(name, validators, primary_name,
                                      self._timer, InternalBus(), self.network.create_peer(name),
                                      write_manager=FakeSomething(database_manager=None))
                       for name in validators]

    @property
    def timer(self) -> MockTimer:
        return self._timer

    @property
    def network(self) -> SimNetwork:
        return self._network

    @property
    def nodes(self) -> List[ReplicaService]:
        return self._nodes


def some_preprepare(view_no: int, pp_seq_no: int, digest: str) -> PrePrepare:
    return PrePrepare(
        instId=0, viewNo=view_no, ppSeqNo=pp_seq_no, ppTime=1499906903,
        reqIdr=[], discarded="", digest=digest,
        ledgerId=1, stateRootHash=None, txnRootHash=None,
        sub_seq_no=0, final=True
    )


def some_random_preprepare(random: SimRandom, view_no: int, pp_seq_no: int) -> PrePrepare:
    return some_preprepare(view_no, pp_seq_no, random.string(40))


def some_checkpoint(random: SimRandom, view_no: int, pp_seq_no: int) -> Checkpoint:
    return Checkpoint(
        instId=0, viewNo=view_no, seqNoStart=pp_seq_no, seqNoEnd=pp_seq_no, digest=random.string(40)
    )


def some_pool(random: SimRandom) -> (SimPool, List):
    pool_size = random.integer(4, 8)
    pool = SimPool(pool_size, random)

    # Create simulated history
    # TODO: Move into helper?
    faulty = (pool_size - 1) // 3
    seq_no_per_cp = 10
    max_batches = 50
    batches = [some_random_preprepare(random, 0, n) for n in range(1, max_batches)]
    checkpoints = [some_checkpoint(random, 0, n) for n in range(0, max_batches, seq_no_per_cp)]

    # Preprepares
    pp_count = [random.integer(0, len(batches)) for _ in range(pool_size)]
    max_pp = sorted(pp_count)[faulty]
    # Prepares
    p_count = [random.integer(0, min(max_pp, pp)) for pp in pp_count]
    max_p = sorted(p_count)[faulty]
    # Checkpoints
    cp_count = [1 + random.integer(0, min(max_p, p)) // seq_no_per_cp for p in pp_count]
    max_stable_cp_indx = sorted(cp_count)[faulty] - 1
    stable_cp = [checkpoints[random.integer(0, min(max_stable_cp_indx, cp))].seqNoEnd for cp in cp_count]

    # Initialize consensus data
    for i, node in enumerate(pool.nodes):
        node._data.preprepared = batches[:pp_count[i]]
        node._data.prepared = batches[:p_count[i]]
        node._data.checkpoints = checkpoints[:cp_count[i]]
        node._data.stable_checkpoint = stable_cp[i]

    committed = []
    for i in range(1, max_batches):
        prepare_count = sum(1 for node in pool.nodes if i <= len(node._data.prepared))
        has_prepared_cert = prepare_count >= pool_size - faulty
        if has_prepared_cert:
            committed.append(ViewChangeService.batch_id(batches[i - 1]))

    return pool, committed


def calc_committed(view_changes, max_pp_seq_no, n, f) -> List[BatchID]:
    def check_in_batch(batch_id, some_batch_id, check_view_no=False):
        if check_view_no and (batch_id[0] != some_batch_id[0]):
            return False
        return batch_id[1] == some_batch_id[1] and batch_id[2] == some_batch_id[2]

    def check_prepared_in_vc(vc, batch_id):
        # check that (pp_seq_no, digest) is present in VC's prepared and preprepared
        for p_batch_id in vc.prepared:
            if not check_in_batch(batch_id, p_batch_id, check_view_no=True):
                continue
            for pp_batch_id in vc.preprepared:
                if check_in_batch(batch_id, pp_batch_id, check_view_no=True):
                    return True

        return False

    def find_batch_id(pp_seq_no):
        for vc in view_changes:
            for batch_id in vc.prepared:
                if batch_id[1] != pp_seq_no:
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
