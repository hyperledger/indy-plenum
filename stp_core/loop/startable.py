from enum import IntEnum, unique

# TODO: move it to plenum-util repo


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
    def hungry(cls):
        """
        Return a tuple of starting and started_hungry
        """
        return cls.starting, cls.started_hungry

    @classmethod
    def ready(cls):
        """
        Return a tuple of started_hungry and started
        """
        return cls.started_hungry, cls.started
