from typing import Callable, Sequence

from plenum.common.exceptions import DidMethodNotFound
from plenum.common.signer_did import DidSigner
from stp_core.types import Identifier

Seed = str
SignerConstructor = Callable[[Identifier], Seed]


class DidMethod:
    def __init__(self,
                 name,
                 pattern,
                 signerConstructor: SignerConstructor=None):
        self.name = name
        self.pattern = pattern
        self.signerConstructor = signerConstructor or DidSigner

    def newSigner(self, identifier, seed):
        return self.signerConstructor(identifier=identifier, seed=seed)


PlenumDidMethod = DidMethod('plenum', 'did:plenum:')


class DidMethods:
    def __init__(self, *didMethods: Sequence[DidMethod]):
        self.d = {}
        for dm in didMethods:
            self.d[dm.name] = dm
        self.default = didMethods[0] if didMethods else None

    def get(self, didMethodName, required=True) -> DidMethod:
        """
        :param didMethodName: name of DID Method
        :param required: if not found and True, throws an exception, else None
        :return: DID Method
        """
        dm = self.d.get(didMethodName) if didMethodName else self.default
        if not dm and required:
            raise DidMethodNotFound
        return dm


DefaultDidMethods = DidMethods(PlenumDidMethod)
