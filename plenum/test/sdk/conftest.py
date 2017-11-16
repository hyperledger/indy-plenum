import os
import pytest
from plenum.test.helper import randomText
import json
from indy.pool import create_pool_ledger_config, open_pool_ledger, close_pool_ledger
from indy.wallet import create_wallet, open_wallet, close_wallet
from indy.signus import create_and_store_my_did
from indy.ledger import sign_and_submit_request
import random
from plenum.test.helper import randomOperation
from indy.error import IndyError, ErrorCode


@pytest.fixture()
def sdk_pool_name():
    return "pool_name_" + randomText(13)


@pytest.fixture()
def sdk_wallet_name():
    return "wallet_name_" + randomText(13)


def _gen_pool_handler(looper, work_dir, name):
    txn_file_name = os.path.join(work_dir, "pool_transactions_genesis")
    pool_config = json.dumps({"genesis_txn": str(txn_file_name)})
    looper.run(create_pool_ledger_config(name, pool_config))
    pool_handle = looper.run(open_pool_ledger(name, None))
    return pool_handle


@pytest.fixture()
def sdk_pool_handle(looper, tdirWithPoolTxns, sdk_pool_name):
    pool_handle = _gen_pool_handler(looper, tdirWithPoolTxns, sdk_pool_name)
    yield pool_handle
    looper.run(close_pool_ledger(pool_handle))


def _gen_wallet_handler(looper, pool_name, wallet_name):
    looper.run(create_wallet(pool_name, wallet_name, None, None, None))
    wallet_handle = looper.run(open_wallet(wallet_name, None, None))
    return wallet_handle


@pytest.fixture()
def sdk_wallet_handle(looper, sdk_pool_name, sdk_wallet_name):
    wallet_handle = _gen_wallet_handler(looper, sdk_pool_name, sdk_wallet_name)
    yield wallet_handle
    looper.run(close_wallet(wallet_handle))


def _trustee1_seed():
    return "000000000000000000000000Trustee1"


@pytest.fixture()
def _steward1_seed(poolTxnStewardData):
    _, seed = poolTxnStewardData
    return seed.decode()


@pytest.fixture()
def sdk_wallet_trustee1(looper, sdk_wallet_handle, _steward1_seed):
    (trustee1_did, trustee1_verkey) = looper.run(
        # create_and_store_my_did(sdk_wallet_handle, json.dumps({"seed": _trustee1_seed()})))
        create_and_store_my_did(sdk_wallet_handle, json.dumps({"seed": _steward1_seed})))
    return sdk_wallet_handle, trustee1_did


def sdk_random_op():
    op = randomOperation()
    op.update({"reqId": random.randint(10, 100000)})
    return json.dumps(op)


def sdk_sign_and_submit_req(looper, pool_handle, wallet_handle, sender_did, req):
    try:
        resp = looper.run(sign_and_submit_request(pool_handle, wallet_handle, sender_did, req))
    except IndyError as e:
        resp = e.error_code
    return resp
