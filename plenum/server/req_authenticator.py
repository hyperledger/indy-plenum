from copy import deepcopy
from typing import Optional

from plenum.common.constants import TXN_TYPE
from common.error import error
from plenum.common.exceptions import NoAuthenticatorFound
from plenum.common.types import OPERATION
from plenum.server.client_authn import ClientAuthNr


class ReqAuthenticator:
    """
    Maintains a list of authenticators. The first authenticator in the list
    of authenticators is the core authenticator
    """
    def __init__(self):
        self._authenticators = []

    def register_authenticator(self, authenticator: ClientAuthNr):
        self._authenticators.append(authenticator)

    def authenticate(self, req_data):
        """
        Authenticates a given request data by verifying signatures from
        any registered authenticators. If the request is a query returns
        immediately, if no registered authenticator can authenticate then an
        exception is raised.
        :param req_data:
        :return:
        """
        identifiers = set()
        typ = req_data.get(OPERATION, {}).get(TXN_TYPE)
        for authenticator in self._authenticators:
            if authenticator.is_query(typ):
                return set()
            if not (authenticator.is_write(typ) or
                    authenticator.is_action(typ)):
                continue
            rv = authenticator.authenticate(deepcopy(req_data)) or set()
            identifiers.update(rv)

        if not identifiers:
            raise NoAuthenticatorFound
        return identifiers

    @property
    def core_authenticator(self):
        if not self._authenticators:
            error('No authenticator registered yet', RuntimeError)
        return self._authenticators[0]

    def get_authnr_by_type(self, authnr_type) -> Optional[ClientAuthNr]:
        for authnr in self._authenticators:
            if isinstance(authnr, authnr_type):
                return authnr
