from copy import deepcopy
from typing import Optional

from plenum.common.constants import TXN_TYPE
from common.error import error
from plenum.common.exceptions import NoAuthenticatorFound
from plenum.common.request import Request
from plenum.common.types import OPERATION, f
from plenum.server.client_authn import ClientAuthNr


class ReqAuthenticator:
    """
    Maintains a list of authenticators. The first authenticator in the list
    of authenticators is the core authenticator
    """
    def __init__(self):
        self._authenticators = []
        self._verified_reqs = {}    # type: Dict[str, Dict[str, List[str]]]

    def register_authenticator(self, authenticator: ClientAuthNr):
        self._authenticators.append(authenticator)

    def authenticate(self, req_data, key=None):
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
        if key and self._check_and_verify_existing_req(req_data, key):
            return self._verified_reqs[key]['identifiers']

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
        if key:
            self._verified_reqs[key] = {'signature': req_data.get(f.SIG.nm)}
            self._verified_reqs[key]['identifiers'] = identifiers
        return identifiers

    def _check_and_verify_existing_req(self, req_data: dict, key: str):
        if key in self._verified_reqs:
            if req_data.get(f.SIG.nm) == self._verified_reqs[key]['signature']:
                return True
        return False

    @property
    def core_authenticator(self):
        if not self._authenticators:
            error('No authenticator registered yet', RuntimeError)
        return self._authenticators[0]

    def get_authnr_by_type(self, authnr_type) -> Optional[ClientAuthNr]:
        for authnr in self._authenticators:
            if isinstance(authnr, authnr_type):
                return authnr

    def clean_from_verified(self, key):
        if key in self._verified_reqs:
            self._verified_reqs.pop(key)
