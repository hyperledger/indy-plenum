from plenum.common.member.member import Member


class Trustee(Member):
    """
    Provides a context for Trustee operations.
    """

    def __init__(self, name=None, wallet=None):
        self.name = name or 'Trustee' + str(id(self))
        self._wallet = wallet
        self.node = None
