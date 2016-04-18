class ClientManager:
    # TODO Should probably have a method for generating stack?
    def __init__(self, authenticator):
        self.authenticator = authenticator

    def bootstrapNewClient(self, identifier, verkey, pubkey=None):
        pass

    def getClientVerkey(self, client):
        pass


# class SimpleClientManager(ClientManager):
#     pass