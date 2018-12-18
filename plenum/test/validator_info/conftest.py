import pytest

from plenum.common.constants import TXN_TYPE, DATA, \
    CURRENT_PROTOCOL_VERSION, DOMAIN_LEDGER_ID
from plenum.common.request import Request
from plenum.common.types import f
from plenum.common.util import getTimeBasedId
from plenum.test.helper import sdk_sign_and_submit_req_obj, sdk_get_and_check_replies

TEST_NODE_NAME = 'Alpha'
INFO_FILENAME = '{}_info.json'.format(TEST_NODE_NAME.lower())
PERIOD_SEC = 1


@pytest.fixture(scope='function')
def info(node):
    return node._info_tool.info


@pytest.fixture(scope='module')
def node(txnPoolNodeSet):
    for n in txnPoolNodeSet:
        if n.name == TEST_NODE_NAME:
            return n
    assert False, 'Pool does not have "{}" node'.format(TEST_NODE_NAME)


@pytest.fixture
def read_txn_and_get_latest_info(looper,
                                 sdk_pool_handle,
                                 sdk_wallet_client, node):
    _, did = sdk_wallet_client
    def read_wrapped(txn_type):
        op = {
            TXN_TYPE: txn_type,
            f.LEDGER_ID.nm: DOMAIN_LEDGER_ID,
            DATA: 1
        }
        req = Request(identifier=did,
                      operation=op, reqId=getTimeBasedId(),
                      protocolVersion=CURRENT_PROTOCOL_VERSION)
        sdk_get_and_check_replies(looper, [sdk_sign_and_submit_req_obj(
            looper, sdk_pool_handle, sdk_wallet_client, req)])

        return node._info_tool.info

    return read_wrapped


@pytest.fixture(scope="function")
def load_latest_info(node):
    def wrapped():
        return node._info_tool.info

    return wrapped
