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
        self.prepare = Quorum(n - f - 1)
        self.commit = Quorum(n - f)
        self.reply = Quorum(f + 1)
        self.view_change = Quorum(n - f)
        self.election = Quorum(2 * f + 1)
