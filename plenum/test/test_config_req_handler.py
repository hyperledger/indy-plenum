import json
from typing import Optional

from plenum.common.constants import TXN_TYPE, DATA, CONFIG_LEDGER_ID
from plenum.common.request import Request
from plenum.common.txn_util import get_type, get_payload_data
from plenum.common.types import f
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.read_request_handler import ReadRequestHandler
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler
from plenum.test.test_node import TestNodeBootstrap


WRITE_CONF = 'write_conf'
READ_CONF = 'read_conf'


class WriteConfHandler(WriteRequestHandler):
    def __init__(self, database_manager: DatabaseManager):
        super().__init__(database_manager=database_manager,
                         txn_type=WRITE_CONF,
                         ledger_id=CONFIG_LEDGER_ID)

    def dynamic_validation(self, request: Request, req_pp_time: Optional[int]):
        pass

    def static_validation(self, request: Request):
        pass

    def update_state(self, txn, prev_result, request, is_committed=False):
        typ = get_type(txn)
        if typ == WRITE_CONF:
            conf = json.loads(get_payload_data(txn)[DATA])
            key, val = conf.popitem()
            self.state.set(key.encode(), val.encode())


class ReadConfHandler(ReadRequestHandler):
    def __init__(self, database_manager: DatabaseManager):
        super().__init__(database_manager, READ_CONF, CONFIG_LEDGER_ID)

    def get_result(self, request: Request):
        key = request.operation[DATA]
        val = self.state.get(key.encode())
        return {f.IDENTIFIER.nm: request.identifier,
                f.REQ_ID.nm: request.reqId,
                **{DATA: json.dumps({key: val.decode()})}}


class ConfigTestBootstrapClass(TestNodeBootstrap):

    def _register_config_req_handlers(self):
        super()._register_config_req_handlers()
        write_rh = WriteConfHandler(self.node.db_manager)
        read_rh = ReadConfHandler(self.node.db_manager)
        self.node.write_manager.register_req_handler(write_rh)
        self.node.read_manager.register_req_handler(read_rh)


def write_conf_op(key, value):
    return {
        TXN_TYPE: WRITE_CONF,
        DATA: json.dumps({key: value})
    }


def read_conf_op(key):
    return {
        TXN_TYPE: READ_CONF,
        DATA: key
    }
