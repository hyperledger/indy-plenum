from indy.ledger import build_txn_author_agreement_request

from plenum.common.constants import CONFIG_LEDGER_ID
from plenum.common.util import randomString
from plenum.server.config_req_handler import ConfigReqHandler


async def prepare_txn_author_agreement(did: str):
    text = randomString(1024)
    version = randomString(16)
    return await build_txn_author_agreement_request(did, text, version)


def get_config_req_handler(node):
    config_req_handler = node.get_req_handler(CONFIG_LEDGER_ID)
    assert isinstance(config_req_handler, ConfigReqHandler)
    return config_req_handler
