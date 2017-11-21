import os
import shutil
import pytest
from typing import Sequence
from plenum.common.constants import CURRENT_PROTOCOL_VERSION
from plenum.test.helper import randomText
from plenum.common.request import Request
import json
from indy.pool import create_pool_ledger_config, open_pool_ledger, close_pool_ledger
from indy.wallet import create_wallet, open_wallet, close_wallet
from indy.signus import create_and_store_my_did
from indy.ledger import sign_and_submit_request, sign_request, submit_request
import random
from plenum.test.helper import random_requests, random_request_objects
from indy.error import IndyError, ErrorCode
import asyncio


@pytest.fixture()
def sdk_pool_name():
    p_name = "pool_name_" + randomText(13)
    yield p_name
    p_dir = os.path.join(os.path.expanduser("~/.indy_client/pool"), p_name)
    if os.path.isdir(p_dir):
        shutil.rmtree(p_dir, ignore_errors=True)


@pytest.fixture()
def sdk_wallet_name():
    w_name = "wallet_name_" + randomText(13)
    yield w_name
    w_dir = os.path.join(os.path.expanduser("~/.indy_client/wallet"), w_name)
    if os.path.isdir(w_dir):
        shutil.rmtree(w_dir, ignore_errors=True)


async def _gen_pool_handler(work_dir, name):
    txn_file_name = os.path.join(work_dir, "pool_transactions_genesis")
    pool_config = json.dumps({"genesis_txn": str(txn_file_name)})
    await create_pool_ledger_config(name, pool_config)
    pool_handle = await open_pool_ledger(name, None)
    return pool_handle


@pytest.fixture()
def sdk_pool_handle(looper, txnPoolNodeSet, tdirWithPoolTxns, sdk_pool_name):
    pool_handle = looper.loop.run_until_complete(_gen_pool_handler(tdirWithPoolTxns, sdk_pool_name))
    yield pool_handle
    looper.loop.run_until_complete(close_pool_ledger(pool_handle))


async def _gen_wallet_handler(pool_name, wallet_name):
    await create_wallet(pool_name, wallet_name, None, None, None)
    wallet_handle = await open_wallet(wallet_name, None, None)
    return wallet_handle


@pytest.fixture()
def sdk_wallet_handle(looper, sdk_pool_name, sdk_wallet_name):
    wallet_handle = looper.loop.run_until_complete(_gen_wallet_handler(sdk_pool_name, sdk_wallet_name))
    yield wallet_handle
    looper.loop.run_until_complete(close_wallet(wallet_handle))


@pytest.fixture()
def sdk_steward_seed(poolTxnStewardData):
    _, seed = poolTxnStewardData
    return seed.decode()


@pytest.fixture()
def sdk_wallet_steward(looper, sdk_wallet_handle, sdk_steward_seed):
    (steward_did, _) = looper.loop.run_until_complete(create_and_store_my_did(sdk_wallet_handle, json.dumps({"seed": sdk_steward_seed})))
    return sdk_wallet_handle, steward_did


def sdk_random_request_objects(count, protocol_version, identifier=None):
    ops = random_requests(count)
    return [Request(operation=op, reqId=random.randint(10, 100000),
                    protocolVersion=protocol_version, identifier=identifier) for op in ops]


def sdk_sign_request_objects(looper, wallet_h, did, reqs: Sequence):
    reqs_str = [json.dumps(req.as_dict) for req in reqs]
    resp = [looper.loop.run_until_complete(sign_request(wallet_h, did, req)) for req in reqs_str]
    return resp


def sdk_signed_random_requests(looper, wallet_h, did, count):
    reqs_obj = sdk_random_request_objects(count, identifier=did, protocol_version=CURRENT_PROTOCOL_VERSION)
    return sdk_sign_request_objects(looper, wallet_h, did, reqs_obj)


def sdk_send_signed_requests(pool_h, signed_reqs: Sequence):
    return [asyncio.ensure_future(submit_request(pool_h, req)) for req in signed_reqs]


def sdk_send_random_requests(looper, pool_h, wallet_h, did, count: int):
    reqs = sdk_signed_random_requests(looper, wallet_h, did, count)
    return sdk_send_signed_requests(pool_h, reqs)


def sdk_send_random_request(looper, pool_h, wallet_h, did):
    rets = sdk_send_random_requests(looper, pool_h, wallet_h, did, 1)
    return rets[0]


def sdk_sign_and_submit_req(pool_handle, wallet_handle, sender_did, req):
    return asyncio.ensure_future(sign_and_submit_request(pool_handle, wallet_handle, sender_did, req))
