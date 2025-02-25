

async def get_did_signing_key(wallet_handle, did):
    item = await wallet_handle.fetch("did", did, for_update=False)
    if item:
        kp = await wallet_handle.fetch_key(item.value_json.get("verkey"))
        return kp.key
    return None

async def sign_request(wallet_handle, submitter_did, req):
    key = await get_did_signing_key(wallet_handle, submitter_did)
    if not key:
        raise Exception(f"Key for DID {submitter_did} is empty")
    req.set_signature(key.sign_message(req.signature_input))
    return req

async def sign_and_submit_request(pool_handle, wallet_handle, submitter_did, req):
    sreq = await sign_request(wallet_handle, submitter_did, req)
    request_result = await pool_handle.submit_request(sreq)
    return request_result