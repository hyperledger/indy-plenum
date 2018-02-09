

class Blacklister:
    """
    Interface to be subclassed by all Blacklister types
    """
    def blacklist(self, name):
        """Add the given name to this blacklister's blacklist"""
        raise NotImplementedError

    def isBlacklisted(self, name) -> bool:
        """Check if the given name exists in this blacklister's blacklist"""
        raise NotImplementedError


class SimpleBlacklister(Blacklister):
    def __init__(self, name):
        self.name = name
        self.blacklisted = set()    # type: Set[str]

    def blacklist(self, name):
        self.blacklisted.add(name)

    def isBlacklisted(self, name):
        return name in self.blacklisted
