from plenum.common.util import getMaxFailures


class Quorum:

    def __init__(self, value: int):
        self.value = value

    def is_reached(self, items: int) -> bool:
        return items >= self.value


class Quorums:

    def __init__(self, n):
        f = getMaxFailures(n)
        self.propagate = Quorum(f + 1)
        self.prepare = Quorum(2 * f)
        self.commit = Quorum(n - f)
        self.reply = Quorum(f + 1)
        self.instance_change = Quorum(2 * f + 1)
        self.election = Quorum(2 * f + 1)
