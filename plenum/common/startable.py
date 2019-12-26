from enum import IntEnum, unique


@unique
class Status(IntEnum):
    """
    Status of a node.

    Members: (serial number corresponds to enum code)

    1. stopped: instance stopped
    2. starting: looking for enough other nodes to start the instance
    3. started_hungry: instance started, but still looking for more nodes
    4. started: instance started, no longer seeking more instance nodes
    5. stopping: instance stopping

    """
    stopped = 1
    starting = 2
    started_hungry = 3
    started = 4
    stopping = 5

    @classmethod
    def going(cls):
        """
        Return a tuple of starting, started_hungry and started
        """
        return cls.starting, cls.started_hungry, cls.started

    @classmethod
    def ready(cls):
        """
        Return a tuple of started_hungry and started
        """
        return cls.started_hungry, cls.started


@unique
class Mode(IntEnum):
    """
    Mode a node can be in
    """
    # TODO: This assumes Pool ledger is the first ledger and Domain ledger
    starting = 100
    discovering = 200     # catching up on pool txn ledger
    discovered = 300      # caught up with pool txn ledger
    syncing = 400         # catching up on domain txn ledger
    synced = 410          # caught up with domain txn ledger
    participating = 500   # caught up completely and chosen primary

    @classmethod
    def is_done_syncing(cls, mode):
        if mode is None:
            return False
        return mode >= cls.synced
