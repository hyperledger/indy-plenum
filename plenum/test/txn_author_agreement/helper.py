from indy.ledger import build_txn_author_agreement_request

from plenum.common.constants import CONFIG_LEDGER_ID
from plenum.server.config_req_handler import ConfigReqHandler
from plenum.test.helper import sdk_sign_and_submit_req, sdk_get_and_check_replies


def sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet, text: str, version: str):
    req = looper.loop.run_until_complete(build_txn_author_agreement_request(sdk_wallet[1], text, version))
    rep = sdk_sign_and_submit_req(sdk_pool_handle, sdk_wallet, req)
    return sdk_get_and_check_replies(looper, [rep])


def get_config_req_handler(node):
    config_req_handler = node.get_req_handler(CONFIG_LEDGER_ID)
    assert isinstance(config_req_handler, ConfigReqHandler)
    return config_req_handler
