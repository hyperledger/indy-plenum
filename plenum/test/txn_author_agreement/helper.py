from random import randint

from plenum.common.constants import TXN_TYPE, CURRENT_PROTOCOL_VERSION, TXN_AUTHOR_AGREEMENT, CONFIG_LEDGER_ID
from plenum.common.types import OPERATION, f
from plenum.common.util import randomString
from plenum.server.config_req_handler import ConfigReqHandler


def gen_txn_author_agreement(did: str, version: str, text: str):
    return {
        OPERATION: {
            TXN_TYPE: TXN_AUTHOR_AGREEMENT,
            'text': text,
            'version': version
        },
        f.IDENTIFIER.nm: did,
        f.REQ_ID.nm: randint(1, 2147483647),
        f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION
    }


def gen_random_txn_author_agreement(did: str):
    text = randomString(1024)
    version = randomString(16)
    return gen_txn_author_agreement(did, version, text)


def get_config_req_handler(node):
    config_req_handler = node.get_req_handler(CONFIG_LEDGER_ID)
    assert isinstance(config_req_handler, ConfigReqHandler)
    return config_req_handler
