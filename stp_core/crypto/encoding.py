import binascii


class RawEncoder(object):

    @staticmethod
    def encode(data):
        return data

    @staticmethod
    def decode(data):
        return data


class HexEncoder(object):

    @staticmethod
    def encode(data):
        return binascii.hexlify(data)

    @staticmethod
    def decode(data):
        return binascii.unhexlify(data)


class Encodable(object):

    def encode(self, encoder=RawEncoder):
        return encoder.encode(bytes(self))
