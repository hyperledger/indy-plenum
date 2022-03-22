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
from plenum.server.plugin.did_plugin.common import DID, NetworkDID, did_id_from_url, libnacl_validate


from plenum.common.txn_util import get_payload_data, get_from, \
    get_seq_no, get_txn_time, get_request_data

import libnacl
import libnacl.encode

"""
CreateDID request structure:

{
    # Mandatory
    "NetworkDIDDocument": {
    "@context": [
        "https://www.w3.org/ns/did/v1",
        "https://w3id.org/security/suites/ed25519-2020/v1"
    ],

    # Mandatory
    "id": "did:iin:iin123:logistics.network",

    # Mandatory
    "networkParticipants": [
    "did:iin:iin123:shippingcompany",
    "did:iin:iin123:oceanliner",
    "did:iin:iin123:seller"
    ],

    "verificationMethod": [
        {
        "id": "did:iin:iin123:logistics.network#multisig",
        "type": "BlockchainNetworkMultiSig",
        "controller": "did:iin:iin123:logistics.network",
        "multisigKeys": [
            "did:iin:iin123:shippingcompany#key1",
            "did:iin:iin123:oceanliner#key1",
            "did:iin:iin123:seller#key1"
        ],
        "updatePolicy": {
            "id": "did:iin:iin123:logistics.network#updatepolicy",
            "controller": "did:iin:iin123:logistics.network",
            "type": "VerifiableCondition2021",
            "conditionAnd": [{
                "id": "did:iin:iin123:logistics.network#updatepolicy-1",
                "controller": "did:iin:iin123:logistics.network",
                "type": "VerifiableCondition2021",
                "conditionOr": ["did:iin:iin123:shippingcompany#key1",
                "did:iin:iin123:seller#key1"
                ]
            },
            "did:iin:iin123:oceanliner#key1"
            ]
        }
        }
    ],
    

    # Mandatory
    "authentication": [
        
        "did:iin:iin123:logistics.network#multisig"
    ],
    
    },

    # Mandatory
    "signatures":{
        "did:iin:iin123:shippingcompany":"...",
        "did:iin:iin123:oceanliner":"...",
        "did:iin:iin123:seller":"..."
    }

}


"""

class CreateNetworkDIDRequest:
    did: NetworkDID = None
    did_str = None
    signatures = None
    this_indy_state = None

    def __init__(self, request_dict: str, indy_state) -> None:
        self.did_str = json.dumps(request_dict["NetworkDIDDocument"])
        self.did = NetworkDID(self.did_str)
        self.signatures = request_dict["signatures"]
        self.this_indy_state = indy_state
    
    def fetch_party_key_from_auth_method(self, party_did_id, auth_method):
        for candidate_key_url in auth_method["multisigKeys"]:
            base_url = did_id_from_url(candidate_key_url)
            if base_url == party_did_id:
                return candidate_key_url

    def fetch_party_verification_method(self, party_key_url):
        party_did_id = did_id_from_url(party_key_url)
        # Fetch party did
        # TODO: if did is in some other iin network

        # If did is in the same indy iin network
        serialized_party_did = self.this_indy_state.get(party_did_id, isCommitted=True)
        if not serialized_party_did:
            raise "Could not resolve did " + party_did_id

        party_did = domain_state_serializer.deserialize(serialized_party_did)
        party_did = DID(party_did)
        party_authentication_method = party_did.fetch_authentication_method(party_key_url)
        return party_authentication_method

    def authenticate(self):
        # Get any one authentication method of type GroupMultiSig
        auth_method = self.did.fetch_authentication_method()

        if not auth_method:
            raise MissingSignature("Authentication verification method not found in NetworkDIDDocument.")
        
        # Iterate of each participant
        for party_did_id in self.did.network_participants:
            # Fetch the key url from auth_method
            party_key_url = self.fetch_party_key_from_auth_method(auth_method, party_did_id)

            # Fetch verification key of the party
            party_verification_method = self.fetch_party_verification_method(party_key_url)

            # Validate signature of the party
            if party_verification_method["type"] == "libnacl":
                # validate signature
                # TODO: Json serialization is not faithful. Use ordered collections isntead.
                originalhash = libnacl.crypto_hash_sha256(self.did_str)
                libnacl_validate(party_verification_method["publicKeyBase64"], self.signatures[party_did_id], originalhash)

                # TODO: Add more authentication methods / some standard
            else:
                raise InvalidSignature("Unknown signature type: ", auth_method["type"])

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
        print("=============")
        print(self.did_str)
        print(originalhash)
        print(verifiedhash)
        if verifiedhash != originalhash:
            # TODO: Json serialization is not faithful. Use ordered collections isntead.
            raise InvalidSignature("The hash of the NetworkDIDDocument did not match.")

class CreateNetworkDIDHandler(AbstractDIDReqHandler):

    def __init__(self, database_manager: DatabaseManager, did_dict: dict):
        super().__init__(database_manager, CREATE_DID, did_dict)

    def additional_dynamic_validation(self, request: Request, req_pp_time: Optional[int]):

        operation = request.operation
        create_network_did_request_dict = operation.get(DATA)
        
        # parse create did request
        try:
            create_network_did_request = CreateNetworkDIDRequest(create_network_did_request_dict, self.state)
        except:
            raise InvalidClientRequest(request.identifier, request.reqId, "Malformed CREATE_NETWORK_DID request.")

        # TODO Check if the did uri corresponds to this iin or not.

        # Check if did already in this iin or not.
        serialized_did = self.state.get(create_network_did_request.did.id, isCommitted=True)
        if serialized_did:
            raise InvalidClientRequest(request.identifier, request.reqId, "DID already exists.")

        # Authenticate
        create_network_did_request.authenticate()



    def update_state(self, txn, prev_result, request, is_committed=False):
        data = get_payload_data(txn).get(DATA)
        create_network_did_request = CreateDIDRequest(data)

        self.did_dict[create_network_did_request.did.id] = create_network_did_request.did_str
        key = create_network_did_request.did.id
        val = self.did_dict[create_network_did_request.did.id]
        print("Setting state:", key, val)
        self.state.set(key.encode(), val)
        return val
