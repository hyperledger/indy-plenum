import libnacl.sign

from typing import Optional
import json

from plenum.common.constants import DATA
from plenum.common.request import Request
from common.serializers.serialization import domain_state_serializer
from plenum.common.exceptions import InvalidClientRequest, MissingSignature, InvalidSignature

from plenum.server.database_manager import DatabaseManager
from plenum.server.plugin.did_plugin.constants import CREATE_DID
from plenum.server.plugin.did_plugin.request_handlers.abstract_did_req_handler import AbstractDIDReqHandler

from plenum.common.txn_util import get_payload_data, get_from, \
    get_seq_no, get_txn_time, get_request_data

import libnacl
import libnacl.encode

"""
CreateDID request structure:

{
    # Mandatory
    "DIDDocument": {
    "@context": [
        "https://www.w3.org/ns/did/v1",
        "https://w3id.org/security/suites/ed25519-2020/v1"
    ],

    # Mandatory
    "id": "did:iin:iin123:tradelens",

    "verificationMethod": [{
        "id": "did:iin:iin123:tradelens#key-1",
        "type": "Ed25519VerificationKey2020", 
        "controller": "did:example:123456789abcdefghi",
        "publicKeyBase64": "zH3C2AVvLMv6gmMNam3uVAjZpfkcJCwDwnZn6z3wXmqPV"
        }
    ],

    # Mandatory
    "authentication": [
        
        "did:iin:iin123:tradelens#keys-1",
        
        {
        "id": "did:iin:iin123:tradelens#keys-2",
        "type": "Ed25519VerificationKey2020",
        "controller": "did:tradelens",
        "publicKeyBase64": "zH3C2AVvLMv6gmMNam3uVAjZpfkcJCwDwnZn6z3wXmqPV"
        }
    ],
    
    },

    # Mandatory
    "signature":{
        "verificationMethod": "did:iin:iin123:tradelens#keys-1",
        "sigbase64": "sdfsdfsdf"
    }

}


"""

class DID:
    did = None
    id = None
    verification_methods = None
    authentication_methods = None

    def __init__(self, did_json) -> None:
        self.did = json.loads(did_json)
        self.id = self.did["id"]

        # populate verification methods:
        self.verification_methods = {}
        for method in self.did["verificationMethod"]:
            self.verification_methods[method["id"]] = method
        
        # populate authentication methods:
        self.authentication_methods = {}
        for method in self.did["authentication"]:
            if isinstance(method, dict):
                # fully specified method
                self.authentication_methods[method["id"]] = method
            elif isinstance(method, str):
                # id points to a verification method
                # TODO: if it points to a different did -> resolve that did and fetch method
                if method in self.verification_methods:
                    self.authentication_methods[method] = self.verification_methods[method]

    def fetch_authentication_method(self, authentication_method_id: str) -> dict:
        if authentication_method_id in self.authentication_methods:
            return self.authentication_methods[authentication_method_id]
        return None


class CreateDIDRequest:
    did: DID = None
    did_str = None
    signature = None


    def __init__(self, request_json: str) -> None:
        requestDict = json.loads(request_json)
        self.did_str = json.dumps(requestDict["DIDDocument"])
        self.did = DID(self.did_str)
        self.signature = requestDict["signature"]
    
    def authenticate(self):
        # Get authentication method
        auth_method = self.did.fetch_authentication_method(self.signature["verificationMethod"])

        if not auth_method:
            raise MissingSignature("Authentication verification method not found in DIDDocument.")
        
        if auth_method["type"] == "libnacl":
            # validate signature
            self._libnacl_validate(auth_method["publicKeyBase64"], self.signature["sigbase64"])
            # TODO: Add more authentication methods / some standard
        else:
            raise InvalidSignature("Unknown signature type: ", auth_method["type"])

    def _libnacl_validate(self, vk_base64, signature_base64):
        vk = libnacl.encode.base64_decode(vk_base64)
        signature = libnacl.encode.base64_decode(signature_base64)
        verifiedhash = libnacl.crypto_sign_open(signature, vk)
        originalhash = libnacl.crypto_hash_sha256(self.did_str)
        if verifiedhash != originalhash:
            # TODO: Json serialization is not faithful. Use ordered collections isntead.
            raise InvalidSignature("The hash of the DIDDocument did not match.")

class CreateDIDHandler(AbstractDIDReqHandler):

    def __init__(self, database_manager: DatabaseManager, did_dict: dict):
        super().__init__(database_manager, CREATE_DID, did_dict)
        print("we are here $$$$$$$$$$$$$$$$$$$$")

    def additional_dynamic_validation(self, request: Request, req_pp_time: Optional[int]):

        print("we are here 1$$$$$$$$$$$$$$$$$$$$")
        operation = request.operation
        create_did_request_str = operation.get(DATA)
        
        # parse create did request
        try:
            create_did_request = CreateDIDRequest(create_did_request_str)
        except:
            raise InvalidClientRequest(request.identifier, request.reqId, "Malformed CREATE_DID request.")

        # TODO Check if the did uri corresponds to this iin or not.
        print("we are here 2$$$$$$$$$$$$$$$$$$$$")

        # Check if did already in this iin or not.
        serialized_did = self.state.get(create_did_request.did.id, isCommitted=True)
        if serialized_did:
            raise InvalidClientRequest(request.identifier, request.reqId, "DID already exists.")

        # Authenticate
        create_did_request.authenticate()



    def update_state(self, txn, prev_result, request, is_committed=False):
        data = get_payload_data(txn).get(DATA)
        create_did_request = CreateDIDRequest(data)

        self.did_dict[create_did_request.did.id] = create_did_request.did_str
        key = create_did_request.did.id
        val = self.did_dict[create_did_request.did.id]
        print("Setting state:", key, val)
        self.state.set(key.encode(), val)
        return val
