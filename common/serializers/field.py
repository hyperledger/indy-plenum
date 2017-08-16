class Field:

    def __init__(self, name, encoder, decoder):
        assert name and isinstance(name, str)
        assert encoder and callable(encoder)
        assert decoder and callable(decoder)

        self.name = name
        self.encoder = encoder
        self.decoder = decoder
