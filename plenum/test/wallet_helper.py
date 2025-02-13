import base58
import json

from indy_vdr import ledger, open_pool
from aries_askar import Store, Key, KeyAlg, AskarError, AskarErrorCode
from indy_credx import Schema, CredentialDefinition, RevocationRegistryDefinition
from indy_vdr.error import VdrError

# TODO: This code is copied from indy-test-automation, we should move it to a common place
# and use it from there in both places


def key_helper(seed=None):
    """
    Generate a new keypair and DID
    """
    alg = KeyAlg.ED25519
    if seed:
        keypair = Key.from_secret_bytes(alg, seed)
    else:
        keypair = Key.generate(alg)
    verkey_bytes = keypair.get_public_bytes()
    verkey = base58.b58encode(verkey_bytes).decode("ascii")
    did = base58.b58encode(verkey_bytes[:16]).decode("ascii")
    return keypair, did, verkey


async def key_insert_helper(wallet_handle, keypair, did, verkey):
    '''
    Insert a keypair into the wallet
    '''
    try:
        await wallet_handle.insert_key(verkey, keypair, metadata=json.dumps({}))
    except AskarError as err:
        if err.code == AskarErrorCode.DUPLICATE:
            pass
        else:
            raise err
    item = await wallet_handle.fetch("did", did, for_update=True)
    if item:
        did_info = item.value_json
        if did_info.get("verkey") != verkey:
            raise Exception("DID already present in wallet")
        did_info["metadata"] = {}
        await wallet_handle.replace("did", did, value_json=did_info, tags=item.tags)
    else:
        await wallet_handle.insert(
            "did",
            did,
            value_json={
                "did": did,
                "method": "sov",
                "verkey": verkey,
                "verkey_type": "ed25519",
                "metadata": {},
            },
            tags={
                "method": "sov",
                "verkey": verkey,
                "verkey_type": "ed25519",
            },
        )


async def create_and_store_did(wallet_handle, seed=None):
    '''
    Create a new DID and store it in the wallet
    '''
    keypair, did, verkey = key_helper(seed=seed)
    await key_insert_helper(wallet_handle, keypair, did, verkey)
    return did, verkey

async def wallet_helper(wallet_key='', wallet_key_derivation_method='kdf:argon2i:mod'):
    wuri = "sqlite://:memory:"
    wallet_h = await Store.provision(wuri, wallet_key_derivation_method, wallet_key, recreate=False)
    session_handle = await wallet_h.session()
    wallet_config = json.dumps({"id": wuri})
    wallet_credentials = json.dumps({"key": wallet_key, "key_derivation_method": wallet_key_derivation_method})

    return session_handle, wallet_config, wallet_credentials
