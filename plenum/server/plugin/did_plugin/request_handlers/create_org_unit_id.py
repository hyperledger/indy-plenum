import libnacl.sign
import nacl
from typing import Optional
import json
from plenum.common.constants import DATA
from plenum.common.request import Request
from common.serializers.serialization import domain_state_serializer
from plenum.common.exceptions import InvalidClientRequest, MissingSignature, InvalidSignature
from plenum.server.database_manager import DatabaseManager
from plenum.server.plugin.did_plugin.constants import OUDID
from plenum.server.plugin.did_plugin.request_handlers.abstract_did_req_handler import AbstractDIDReqHandler
from plenum.server.plugin.did_plugin.common import DID, libnacl_validate
from plenum.common.txn_util import get_payload_data, get_from, \
    get_seq_no, get_txn_time, get_request_data

import libnacl

import libnacl.encode
print("hello1")

"""
DID identifier (globally unique)::> Stencil: did:<method-name>:<method-specific-id>
                                        Ex.: did:exampleiin:org1

{
  "id": "did:exampleiin:org1",
  "verificationMethod": [{
    "id": "did:exampleiin:org1#key1",
    "type": "Ed25519VerificationKey2020",
    "controller": "did:exampleiin:org1",
    "publicKeyMultibase": "zH3C2AVvLMv6gmMNam3uVAjZpfkcJCwDwnZn6z3wXmqPV"
  }],

  "authentication": ["did:exampleiin:org1#key1"]
}


================================
| CreateDID request structure: |
================================

=================================================================================
|| {                                                                           ||
||   "DIDDocument": {   "controller                                            ||
||     "id": "did:<method-name>:<method-specific-id>",                         ||          
||     "verificationMethod": [{                                                ||
||       "id": "did:<method-name>:<method-specific-id>",                       ||
||       "type": "Ed25519VerificationKey2020",                                 ||
||       "controller": "did:<method-name>:<method-specific-id>",               ||
||       "publicKeyMultibase": "zH3C2AVvLMv6gmMNam3uVAjZpfkcJCwDwnZn6z3wXmqPV" ||  <=================??????=======
||     }],                                                                     ||
||                                                                             ||
||     "authentication": ["did:<method-name>:<method-specific-id>"]            ||
||   },                                                                        ||
||   "signature": {"did:iin:<method-name>:<method-specific-id>": "..."}        ||    
|| }                                                                           ||
=================================================================================


"""

class CreateOUDIDRequest:
    did: DID = None
    did_str = None
    signature = None

    def __init__(self, request_dict: str) -> None:
        self.did_str = json.dumps(request_dict["DIDDocument"])
        self.did = DID(self.did_str)
        self.signature = request_dict["signature"]

    def authenticate(self):
        # Get authentication method
        print(self.signature)
        auth_method = self.did.fetch_authentication_method(self.signature["verificationMethod"])
        print(f"=========={self.signature}==========")
        print(auth_method)
        print(auth_method["type"])
        if not auth_method:
            raise MissingSignature("Authentication verification method not found in DIDDocument.")
        if auth_method["type"] == "libnacl":
            # validate signature
            # TODO: Json serialization is not faithful. Use ordered collections isntead.
            originalhash = libnacl.crypto_hash_sha256(self.did_str)
            # TAG::>
            libnacl_validate(auth_method["publicKeyMultibase"], self.signature["sigbase64"], originalhash)
            # TODO: Add more authentication methods / some standard
        else:
            raise InvalidSignature("Unknown signature type: ", auth_method["type"])



class CreateOUDIDHandler(AbstractDIDReqHandler):

    def __init__(self, database_manager: DatabaseManager, did_dict: dict):
        super().__init__(database_manager, OUDID, did_dict)

    def additional_dynamic_validation(self, request: Request, req_pp_time: Optional[int]):

        operation = request.operation
        create_did_request_dict = operation.get(DATA)
        
        # parse create did request
        try:
            create_did_request = CreateOUDIDRequest(create_did_request_dict)
        except:
            raise InvalidClientRequest(request.identifier, request.reqId, "Malformed OUDID request.")

        # TODO Check if the did uri corresponds to this iin or not.

        # Check if did already in this iin or not.
        # serialized_did = self.state.get(create_did_request.did.id, isCommitted=False)
        # if serialized_did:
        #     raise InvalidClientRequest(request.identifier, request.reqId, "DID already exists.")

        # Authenticate
        create_did_request.authenticate()


    def update_state(self, txn, prev_result, request, is_committed=False):
        data = get_payload_data(txn).get(DATA)
        create_did_request = CreateOUDIDRequest(data)

        self.did_dict[create_did_request.did.id] = create_did_request.did_str
        key = create_did_request.did.id
        val = self.did_dict[create_did_request.did.id]
        print("Setting state:", key, val)
        self.state.set(key.encode(), val)
        return val