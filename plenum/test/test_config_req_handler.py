import json

from plenum.common.constants import TXN_TYPE, DATA
from plenum.common.request import Request
from plenum.common.txn_util import reqToTxn
from plenum.common.types import f
from plenum.persistence.util import txnsWithSeqNo
from plenum.server.config_req_handler import ConfigReqHandler

WRITE_CONF = 'write_conf'
READ_CONF = 'read_conf'


class TestConfigReqHandler(ConfigReqHandler):
    write_types = {WRITE_CONF, }
    query_types = {READ_CONF, }

    def __init__(self, ledger, state):
        super().__init__(ledger, state)
        self.query_handlers = {
            READ_CONF: self.handle_get_conf,
        }

    def get_query_response(self, request: Request):
        return self.query_handlers[request.operation[TXN_TYPE]](request)

    def apply(self, req: Request, cons_time: int):
        txn = reqToTxn(req, cons_time)
        (start, end), _ = self.ledger.appendTxns(
            [self.transform_txn_for_ledger(txn)])
        self.updateState(txnsWithSeqNo(start, end, [txn]))
        return start, txn

    def updateState(self, txns, isCommitted=False):
        for txn in txns:
            self._updateStateWithSingleTxn(txn, isCommitted=isCommitted)

    def _updateStateWithSingleTxn(self, txn, isCommitted=False):
        typ = txn.get(TXN_TYPE)
        if typ == WRITE_CONF:
            conf = json.loads(txn[DATA])
            key, val = conf.popitem()
            self.state.set(key.encode(), val.encode())

    def handle_get_conf(self, request: Request):
        key = request.operation[DATA]
        val = self.state.get(key.encode())
        return {f.IDENTIFIER.nm: request.identifier,
                f.REQ_ID.nm: request.reqId,
                **{DATA: json.dumps({key: val.decode()})}}


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
