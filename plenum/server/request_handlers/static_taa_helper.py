from _sha256 import sha256
from typing import Optional

from common.serializers.serialization import config_state_serializer
from plenum.common.constants import TXN_TYPE, TXN_AUTHOR_AGREEMENT, TXN_AUTHOR_AGREEMENT_AML, DOMAIN_LEDGER_ID
from plenum.common.exceptions import UnauthorizedClientRequest
from plenum.common.request import Request
from plenum.server.request_handlers.state_constants import MARKER_TAA, MARKER_TAA_AML
from plenum.server.request_handlers.utils import is_trustee


class StaticTAAHelper:

    @staticmethod
    def state_path_taa_latest() -> bytes:
        return "{marker}:latest". \
            format(marker=MARKER_TAA).encode()

    @staticmethod
    def state_path_taa_version(version: str) -> bytes:
        return "{marker}:v:{version}". \
            format(marker=MARKER_TAA, version=version).encode()

    @staticmethod
    def state_path_taa_digest(digest: str) -> bytes:
        return "{marker}:d:{digest}". \
            format(marker=MARKER_TAA, digest=digest).encode()

    @staticmethod
    def taa_digest(text: str, version: str) -> str:
        return sha256('{}{}'.format(version, text).encode()).hexdigest()

    @staticmethod
    def state_path_taa_aml_latest():
        return "{marker}:latest".format(marker=MARKER_TAA_AML).encode()

    @staticmethod
    def state_path_taa_aml_version(version: str):
        return "{marker}:v:{version}".format(marker=MARKER_TAA_AML, version=version).encode()

    @staticmethod
    def get_taa_digest(state, version: Optional[str] = None,
                       isCommitted: bool = True) -> Optional[str]:
        path = StaticTAAHelper.state_path_taa_latest() if version is None \
            else StaticTAAHelper.state_path_taa_version(version)
        res = state.get(path, isCommitted=isCommitted)
        if res is not None:
            return res.decode()

    @staticmethod
    def get_latest_taa(state):
        last_taa_digest = state.get(StaticTAAHelper.state_path_taa_latest(), isCommitted=False)
        if last_taa_digest is None:
            return None
        return last_taa_digest.decode()

    @staticmethod
    def get_taa_aml_data(state, version: Optional[str] = None,
                         isCommitted: bool = True):
        path = StaticTAAHelper.state_path_taa_aml_latest() if version is None \
            else StaticTAAHelper.state_path_taa_aml_version(version)
        payload = state.get(path, isCommitted=isCommitted)
        if payload is None:
            return None
        return config_state_serializer.deserialize(payload)

    @staticmethod
    def get_digest_from_state_key(encode_key):
        return encode_key.decode().rsplit(":", maxsplit=1)[1]

    @staticmethod
    def authorize(database_manager, req: Request):
        typ = req.operation.get(TXN_TYPE)
        domain_state = database_manager.get_database(DOMAIN_LEDGER_ID).state

        if typ in [TXN_AUTHOR_AGREEMENT, TXN_AUTHOR_AGREEMENT_AML] \
                and not is_trustee(domain_state, req.identifier, is_committed=False):
            raise UnauthorizedClientRequest(req.identifier, req.reqId,
                                            "Only trustee can update transaction author agreement and AML")
