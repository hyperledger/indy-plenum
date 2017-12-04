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
from indy.ledger import sign_and_submit_request, sign_request, submit_request, build_nym_request
from indy.error import ErrorCode, IndyError
import random
from plenum.test.helper import random_requests
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
def sdk_client_seed(poolTxnClientData):
    _, seed = poolTxnClientData
    return seed.decode()


@pytest.fixture()
def sdk_new_client_seed():
    return "Client10000000000000000000000000"


@pytest.fixture()
def sdk_wallet_steward(looper, sdk_wallet_handle, sdk_steward_seed):
    (steward_did, steward_verkey) = looper.loop.run_until_complete(
        create_and_store_my_did(sdk_wallet_handle, json.dumps({"seed": sdk_steward_seed})))
    return sdk_wallet_handle, steward_did


@pytest.fixture()
def sdk_wallet_client(looper, sdk_wallet_handle, sdk_client_seed):
    (client_did, _) = looper.loop.run_until_complete(
        create_and_store_my_did(sdk_wallet_handle, json.dumps({"seed": sdk_client_seed})))
    return sdk_wallet_handle, client_did


async def _gen_named_wallet(pool_handle, wallet_steward, named_seed):
    wh, steward_did = wallet_steward
    (named_did, named_verkey) = await create_and_store_my_did(wh, json.dumps({"seed": named_seed, 'cid': True}))
    nym_request = await build_nym_request(steward_did, named_did, named_verkey, None, None)
    await sign_and_submit_request(pool_handle, wh, steward_did, nym_request)
    return wh, named_did


@pytest.fixture()
def sdk_wallet_new_client(looper, sdk_pool_handle, sdk_wallet_steward, sdk_new_client_seed):
    wh, client_did = looper.loop.run_until_complete(
        _gen_named_wallet(sdk_pool_handle, sdk_wallet_steward, sdk_new_client_seed))
    return wh, client_did


def sdk_random_request_objects(count, protocol_version, identifier=None):
    ops = random_requests(count)
    return [Request(operation=op, reqId=random.randint(10, 100000),
                    protocolVersion=protocol_version, identifier=identifier) for op in ops]


def sdk_sign_request_objects(looper, sdk_wallet, reqs: Sequence):
    wallet_h, did = sdk_wallet
    reqs_str = [json.dumps(req.as_dict) for req in reqs]
    resp = [looper.loop.run_until_complete(sign_request(wallet_h, did, req)) for req in reqs_str]
    return resp


def sdk_signed_random_requests(looper, sdk_wallet, count):
    _, did = sdk_wallet
    reqs_obj = sdk_random_request_objects(count, identifier=did, protocol_version=CURRENT_PROTOCOL_VERSION)
    return sdk_sign_request_objects(looper, sdk_wallet, reqs_obj)


def sdk_send_signed_requests(pool_h, signed_reqs: Sequence):
    return [(json.loads(req), asyncio.ensure_future(submit_request(pool_h, req))) for req in signed_reqs]


def sdk_send_random_requests(looper, pool_h, sdk_wallet, count: int):
    reqs = sdk_signed_random_requests(looper, sdk_wallet, count)
    return sdk_send_signed_requests(pool_h, reqs)


def sdk_send_random_request(looper, pool_h, sdk_wallet):
    rets = sdk_send_random_requests(looper, pool_h, sdk_wallet, 1)
    return rets[0]


def sdk_sign_and_submit_req(pool_handle, sdk_wallet, req):
    wallet_handle, sender_did = sdk_wallet
    return json.loads(req), asyncio.ensure_future(sign_and_submit_request(pool_handle, wallet_handle, sender_did, req))


def sdk_get_reply(looper, sdk_req_resp, timeout=None):
    req_json, resp_task = sdk_req_resp
    try:
        resp = looper.run(asyncio.wait_for(resp_task, timeout=timeout))
        resp = json.loads(resp)
    except asyncio.TimeoutError:
        resp = None
    except IndyError as e:
        resp = e.error_code

    return req_json, resp


def sdk_get_replies(looper, sdk_req_resp: Sequence, timeout=None):
    resp_tasks = [resp for _, resp in sdk_req_resp]

    def get_res(task, done_list):
        if task in done_list:
            try:
                resp = json.loads(task.result())
            except IndyError as e:
                resp = e.error_code
        else:
            resp = None
        return resp

    done, pend = looper.run(asyncio.wait(resp_tasks, timeout=timeout))
    ret = [(req, get_res(resp, done)) for req, resp in sdk_req_resp]
    return ret
