from plenum.common.util import getMaxFailures


class Quorum:
    def __init__(self, value: int):
        self.value = value

    def is_reached(self, msg_count: int) -> bool:
        return msg_count >= self.value

    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, self.value)


class Quorums:
    def __init__(self, n):
        f = getMaxFailures(n)
        self.n = n
        self.f = f
        self.weak = Quorum(f + 1)
        self.strong = Quorum(n - f)
        self.propagate = Quorum(f + 1)
        self.prepare = Quorum(n - f - 1)
        self.commit = Quorum(n - f)
        self.reply = Quorum(f + 1)
        self.view_change = Quorum(n - f)
        self.election = Quorum(n - f)
        self.view_change_done = Quorum(n - f)
        self.propagate_primary = Quorum(f + 1)
        self.same_consistency_proof = Quorum(f + 1)
        self.consistency_proof = Quorum(f + 1)
        self.ledger_status = Quorum(n - f - 1)
        self.ledger_status_last_3PC = Quorum(f + 1)
        self.checkpoint = Quorum(n - f - 1)
        self.timestamp = Quorum(f + 1)
        self.bls_signatures = Quorum(n - f)
        self.observer_data = Quorum(f + 1)
        self.backup_instance_faulty = Quorum(f + 1)

    def __str__(self):
        # TODO more robust implementation
        return "{}".format(self.__dict__)
