from common.exceptions import PlenumTypeError

class Field:

    def __init__(self, name, encoder, decoder):

        if not isinstance(name, str):
            raise PlenumTypeError('name', name, str)
        if not name:
            raise ValueError("'name' should be a non-empty string")
        if not callable(encoder):
            raise PlenumTypeError('encoder', encoder, 'callable')
        if not callable(decoder):
            raise PlenumTypeError('decoder', decoder, 'callable')

        self.name = name
        self.encoder = encoder
        self.decoder = decoder
