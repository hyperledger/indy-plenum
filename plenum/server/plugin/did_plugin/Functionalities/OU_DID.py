import json
import base64
from nacl.signing import VerifyKey
from nacl.exceptions import BadSignatureError

"""
1. The function wil recieve REQUEST_BODY somehow
2. The REQUEST_BODY will have `DIDDocment`which will have all the info....
3. I will get `publicKeyMultibase` for verification purposes.
4. Now get the `VerificationKey` and `Signature` from REQUEST_BODY.
5. Then tried to verify it....
"""

request = {
    "identifier": "EbP4aYNeTHL6q385GuVpRV",
    "operation": {
        "dest": "Vzfdscz6YG6n1EuNJV4ob1",
        "type": "20226",
        "data": {
            "DIDDocument": {
                "id": "did:exampleiin:org1",
                "verificationMethod": [
                    {
                        "id": "did:exampleiin:org1#key1",
                        "type": "Ed25519VerificationKey2020",
                        "controller": "did:exampleiin:org1",
                        "publicKeyMultibase": "zH3C2AVvLMv6gmMNam3uVAjZpfkcJCwDwnZn6z3wXmqPV"
                    }
                ],
                "authentication": ["did:exampleiin:org1"] 
            },
            "signature": {
                "verificationMethod": "did:exampleiin:org1#key1",
                "sigbase64": "sdfsdfsdf"
            }
        },
        "verkey": "~HFPBKb7S7ocrTzxakNbcao"
    },
    "protocolVersion": 2,
    "reqId": 1704282737760629997
}

# Get DID document  
did_doc = json.loads(request["operation"]["data"]["DIDDocument"])

# Get public key from DID document
key_id = "did:exampleiin:org1#key1" 
public_key = did_doc["verificationMethod"][0]["publicKeyMultibase"]

# Load public key
verify_key = VerifyKey(base64.standard_b64decode(public_key))

# Get signature  
signature = base64.standard_b64decode(request["operation"]["data"]["signature"]["sigbase64"])

# Get signed data
signed_data = json.dumps(did_doc)

try:
    verify_key.verify(signed_data.encode(), signature)
    print("Signature is valid!")
except BadSignatureError: 
    print("Invalid signature!")