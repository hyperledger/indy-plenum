
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
            State root of pool ledger.
            It is required to let signature be validated even if node keys
            or pool structure changed.
        """

        self.signature = signature
        self.participants = participants
        self.state_root = pool_state_root
