#! /usr/bin/env python3

import argparse
import json
import os
import random
import time
from contextlib import ExitStack
from typing import Sequence

from indy import did, wallet
from indy.ledger import sign_request

from plenum.common.config_util import getConfig
from plenum.common.constants import CURRENT_PROTOCOL_VERSION
from plenum.common.request import Request
from plenum.common.txn_util import reqToTxn, append_txn_metadata
from plenum.common.util import randomString
from stp_core.loop.looper import Looper

config = getConfig()


async def get_wallet_and_pool():
    pool_name = 'pool' + randomString(3)
    wallet_name = 'wallet' + randomString(10)
    their_wallet_name = 'their_wallet' + randomString(10)
    seed_trustee1 = "000000000000000000000000Trustee1"

    await wallet.create_wallet(pool_name, wallet_name, None, None, None)
    my_wallet_handle = await wallet.open_wallet(wallet_name, None, None)

    await wallet.create_wallet(pool_name, their_wallet_name, None, None, None)
    their_wallet_handle = await wallet.open_wallet(their_wallet_name, None, None)

    await did.create_and_store_my_did(my_wallet_handle, "{}")

    (their_did, their_verkey) = await did.create_and_store_my_did(their_wallet_handle,
                                                                  json.dumps({"seed": seed_trustee1}))

    await did.store_their_did(my_wallet_handle, json.dumps({'did': their_did, 'verkey': their_verkey}))

    return their_wallet_handle, their_did


def randomOperation():
    return {
        "type": "buy",
        "amount": random.randint(10, 100000)
    }


def random_requests(count):
    return [randomOperation() for _ in range(count)]


def sdk_gen_request(operation, protocol_version=CURRENT_PROTOCOL_VERSION, identifier=None):
    return Request(operation=operation, reqId=random.randint(10, 100000),
                   protocolVersion=protocol_version, identifier=identifier)


def sdk_random_request_objects(count, protocol_version, identifier=None):
    ops = random_requests(count)
    return [sdk_gen_request(op, protocol_version=protocol_version, identifier=identifier) for op in ops]


def sdk_sign_request_objects(looper, sdk_wallet, reqs: Sequence):
    wallet_h, did = sdk_wallet
    reqs_str = [json.dumps(req.as_dict) for req in reqs]
    resp = [looper.loop.run_until_complete(sign_request(wallet_h, did, req)) for req in reqs_str]
    return resp


def sdk_signed_random_requests(looper, sdk_wallet, count):
    _, did = sdk_wallet
    reqs_obj = sdk_random_request_objects(count, identifier=did, protocol_version=CURRENT_PROTOCOL_VERSION)
    return sdk_sign_request_objects(looper, sdk_wallet, reqs_obj)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('count', help="Count of generated txns", type=int)
    parser.add_argument('outfpath', help="Path to save generated txns", type=str, default='/tmp/generated_txns')
    args = parser.parse_args()
    path_to_save = os.path.realpath(args.outfpath)

    with ExitStack() as exit_stack:
        with Looper() as looper:
            sdk_wallet, DID = looper.loop.run_until_complete(get_wallet_and_pool())
            with open(path_to_save, 'w') as outpath:
                for i in range(args.count):
                    req = sdk_signed_random_requests(looper, (sdk_wallet, DID), 1)[0]
                    txn = reqToTxn(req)
                    append_txn_metadata(txn, txn_time=int(time.time()))
                    outpath.write(json.dumps(txn))
                    outpath.write(os.linesep)
            looper.stopall()
