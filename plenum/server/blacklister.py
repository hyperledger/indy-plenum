from typing import Set


class Blacklister:
    """
    Interface to be subclassed by all Blacklister types
    """
    def blacklist(self, name):
        raise NotImplementedError

    def isBlacklisted(self, name):
        raise NotImplementedError


class SimpleBlacklister(Blacklister):
    def __init__(self, name):
        self.name = name
        self.blacklisted = set()    # type: Set[str]

    def blacklist(self, name):
        self.blacklisted.add(name)

    def isBlacklisted(self, name):
        return name in self.blacklisted
