class MultiSignature:
    """
    Data class for storing multi signature and
    all data required for verification.
    """

    def __init__(self,
                 signature: str,
                 participants: list,
                 pool_state_root: str):
        """
        :param signature: Multi signature itself
        :param participants: List of signers
        :param pool_state_root:
            State root of pool ledger (serialized to str)
            It is required to let signature be validated even if node keys
            or pool structure changed.
        """
        assert signature is not None
        assert participants
        assert pool_state_root is not None
        self.signature = signature
        self.participants = participants
        self.pool_state_root = pool_state_root

    def as_dict(self):
        return self.__dict__

    def __eq__(self, other):
        return isinstance(other, MultiSignature) and self.as_dict() == other.as_dict()
