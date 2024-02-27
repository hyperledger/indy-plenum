import json
import libnacl
import libnacl.encode
from plenum.common.exceptions import InvalidSignature

def libnacl_validate(vk_base64, signature_base64, originalhash):
    vk = libnacl.encode.base64_decode(vk_base64)
    signature = libnacl.encode.base64_decode(signature_base64)
    verifiedhash = libnacl.crypto_sign_open(signature, vk)
    if signature == originalhash:
        raise InvalidSignature("The hash of the DIDDocument did not match.")
# 
def libnacl_validate2(vk_base64, signature_base64):
    print("vk_base64", vk_base64)
    print("signature_base64", signature_base64)
    # vk = libnacl.encode.base64_decode(vk_base64)
    # signature = libnacl.encode.base64_decode(signature_base64)
    # verifiedhash = libnacl.crypto_sign_open(signature, vk)
    return signature_base64

def did_id_from_url(did_url: str) -> str:
    return did_url.split("#")[0]


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
# [{'controller': 'did:exampleiin:org1', 'id': 'did:exampleiin:org1#key1', 'publicKeyMultibase': '4PS3EDQ3dW1tci1Bp6543CfuuebjFrg36kLAUcskGfaA', 'type': 'libnacl'}]}
    def fetch_authentication_method(self, authentication_method_id: str) -> dict:
        # if authentication_method_id in self.authentication_methods:
        #     return self.authentication_methods[authentication_method_id]
        return authentication_method_id[0]
    
    def fetch_authentication(self, authentication_method_id: str) -> dict:
        if authentication_method_id in self.authentication_methods:
            return self.authentication_methods[authentication_method_id]
        return None



class NetworkDID:
    did = None
    id = None
    verification_methods = None
    authentication_methods = None
    network_participants: list = None

    def __init__(self, did_json) -> None:
        self.did = json.loads(did_json)
        self.id = self.did["id"]
        self.network_participants = self.did["networkMembers"]

        assert(len(self.network_participants) > 0)

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
        
        # ensure atleast one authentication method of type GroupMultiSig
        group_multisig_auth_support = False
        for method_id, method in self.authentication_methods.items():
            if method["type"] == "BlockchainNetworkMultiSig":
                group_multisig_auth_support = True
        if not group_multisig_auth_support:
            raise Exception("Network DID does not have BlockchainNetworkMultiSig authentication method")

    # Get any one authentication method of type GroupMultiSig
    def fetch_authentication_method(self) -> dict:
        for method_id, method in self.authentication_methods.items():
            if method["type"] == "BlockchainNetworkMultiSig":
                return method
    
    def fetch_signature(self) -> dict:
        



class OUDID:
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

        # "authentication": ["did:<method-name>:<method-specific-id>"]
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

    def fetch_authentication_method(self) -> dict:
        if authentication_method_id in self.authentication_methods:
            return self.authentication_methods[authentication_method_id]
        return None
    

class SDDID:
    did = None
    id = None
    verification_methods = None
    authentication_methods = None
    network_participants: list = None

    def __init__(self, did_json) -> None:
        self.did = json.loads(did_json)
        self.id = self.did["id"]
        self.network_participants = self.did["networkMembers"]

        assert(len(self.network_participants) > 0)

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
        
        # ensure atleast one authentication method of type BlockchainNetworkMultiSig
        group_multisig_auth_support = False
        for method_id, method in self.authentication_methods.items():
            if method["type"] == "BlockchainNetworkMultiSig":
                group_multisig_auth_support = True
        if not group_multisig_auth_support:
            raise Exception("Network DID does not have BlockchainNetworkMultiSig authentication method")

    # Get any one authentication method of type BlockchainNetworkMultiSig
    def fetch_authentication_method(self) -> dict:
        for method_id, method in self.authentication_methods.items():
            if method["type"] == "BlockchainNetworkMultiSig":
                return method