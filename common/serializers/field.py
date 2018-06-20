from common.exceptions import PlenumTypeError, PlenumValueError


class Field:

    def __init__(self, name, encoder, decoder):

        if not isinstance(name, str):
            raise PlenumTypeError('name', name, str)
        if not name:
            raise PlenumValueError('name', name, 'a non-empty string')
        if not callable(encoder):
            raise PlenumTypeError('encoder', encoder, 'callable')
        if not callable(decoder):
            raise PlenumTypeError('decoder', decoder, 'callable')

        self.name = name
        self.encoder = encoder
        self.decoder = decoder
