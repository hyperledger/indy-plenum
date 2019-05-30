from functools import lru_cache
from typing import Optional

from common.serializers.serialization import pool_state_serializer, config_state_serializer
from plenum.common.constants import POOL_LEDGER_ID, NODE, DATA, BLS_KEY, \
    BLS_KEY_PROOF, TARGET_NYM, DOMAIN_LEDGER_ID, NODE_IP, \
    NODE_PORT, CLIENT_IP, CLIENT_PORT, ALIAS, TXN_AUTHOR_AGREEMENT, CONFIG_LEDGER_ID, TXN_AUTHOR_AGREEMENT_AML, AML, \
    AML_VERSION, GET_TXN_AUTHOR_AGREEMENT_AML
from plenum.common.exceptions import InvalidClientRequest, UnauthorizedClientRequest
from plenum.common.request import Request
from plenum.common.txn_util import get_payload_data, get_from
from plenum.common.types import f
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.read_request_handler import ReadRequestHandler
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler
from plenum.server.request_handlers.static_taa_helper import StaticTAAHelper
from plenum.server.request_handlers.utils import is_steward


class GetTxnAuthorAgreementAmlHandler(ReadRequestHandler):

    def __init__(self, node, database_manager: DatabaseManager):
        super().__init__(database_manager, GET_TXN_AUTHOR_AGREEMENT_AML, None)
        self.node = node

    def get_result(self, request: Request):
        pass
