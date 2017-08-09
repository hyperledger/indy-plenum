from enum import Enum, unique


@unique
class AuthMode(Enum):
    # a client certificate needs to be in the certificates directory
    RESTRICTED = 0

    # allow all client keys without checking
    ALLOW_ANY = 2
