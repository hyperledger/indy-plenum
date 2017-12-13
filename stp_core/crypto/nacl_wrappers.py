import libnacl

from stp_core.crypto import encoding


class SignedMessage(bytes):
    """
    A bytes subclass that holds a messaged that has been signed by a
    :class:`SigningKey`.
    """

    @classmethod
    def _from_parts(cls, signature, message, combined):
        obj = cls(combined)
        obj._signature = signature
        obj._message = message
        return obj

    @property
    def signature(self):
        """
        The signature contained within the :class:`SignedMessage`.
        """
        return self._signature

    @property
    def message(self):
        """
        The message contained within the :class:`SignedMessage`.
        """
        return self._message


class EncryptedMessage(bytes):
    """
    A bytes subclass that holds a messaged that has been encrypted by a
    :class:`SecretBox`.
    """

    @classmethod
    def _from_parts(cls, nonce, ciphertext, combined):
        obj = cls(combined)
        obj._nonce = nonce
        obj._ciphertext = ciphertext
        return obj

    @property
    def nonce(self):
        """
        The nonce used during the encryption of the :class:`EncryptedMessage`.
        """
        return self._nonce

    @property
    def ciphertext(self):
        """
        The ciphertext contained within the :class:`EncryptedMessage`.
        """
        return self._ciphertext


class VerifyKey(encoding.Encodable):
    """
    The public key counterpart to an Ed25519 SigningKey for producing digital
    signatures.

    :param key: [:class:`bytes`] Serialized Ed25519 public key
    :param encoder: A class that is able to decode the `key`
    """

    def __init__(self, key, encoder=encoding.RawEncoder):
        # Decode the key
        key = encoder.decode(key)

        if len(key) != libnacl.crypto_sign_PUBLICKEYBYTES:
            raise ValueError(
                "The key must be exactly %s bytes long" %
                libnacl.crypto_sign_PUBLICKEYBYTES,
            )

        self._key = key

    def __bytes__(self):
        return self._key

    def verify(self, smessage, signature=None, encoder=encoding.RawEncoder):
        """
        Verifies the signature of a signed message, returning the message
        if it has not been tampered with else raising
        :class:`~ValueError`.

        :param smessage: [:class:`bytes`] Either the original messaged or a
            signature and message concated together.
        :param signature: [:class:`bytes`] If an unsigned message is given for
            smessage then the detached signature must be provded.
        :param encoder: A class that is able to decode the secret message and
            signature.
        :rtype: :class:`bytes`
        """
        if signature is not None:
            # If we were given the message and signature separately, combine
            #   them.
            smessage = signature + smessage

        # Decode the signed message
        smessage = encoder.decode(smessage)

        return libnacl.crypto_sign_open(smessage, self._key)


class SigningKey(encoding.Encodable):
    """
    Private key for producing digital signatures using the Ed25519 algorithm.

    Signing keys are produced from a 32-byte (256-bit) random seed value. This
    value can be passed into the :class:`~SigningKey` as a
    :func:`bytes` whose length is 32.

    .. warning:: This **must** be protected and remain secret. Anyone who knows
        the value of your :class:`~SigningKey` or it's seed can
        masquerade as you.

    :param seed: [:class:`bytes`] Random 32-byte value (i.e. private key)
    :param encoder: A class that is able to decode the seed

    :ivar: verify_key: [:class:`~VerifyKey`] The verify
        (i.e. public) key that corresponds with this signing key.
    """

    def __init__(self, seed, encoder=encoding.RawEncoder):
        # Decode the seed
        seed = encoder.decode(seed)

        # Verify that our seed is the proper size
        if len(seed) != libnacl.crypto_sign_SEEDBYTES:
            raise ValueError(
                "The seed must be exactly %d bytes long" %
                libnacl.crypto_sign_SEEDBYTES
            )

        public_key, secret_key = libnacl.crypto_sign_seed_keypair(seed)

        self._seed = seed
        self._signing_key = secret_key
        self.verify_key = VerifyKey(public_key)

    def __bytes__(self):
        return self._seed

    @classmethod
    def generate(cls):
        """
        Generates a random :class:`~SigningKey` object.

        :rtype: :class:`~SigningKey`
        """
        return cls(
            libnacl.randombytes(libnacl.crypto_sign_SEEDBYTES),
            encoder=encoding.RawEncoder,
        )

    def sign(self, message, encoder=encoding.RawEncoder):
        """
        Sign a message using this key.

        :param message: [:class:`bytes`] The data to be signed.
        :param encoder: A class that is used to encode the signed message.
        :rtype: :class:`~SignedMessage`
        """
        raw_signed = libnacl.crypto_sign(message, self._signing_key)

        signature = encoder.encode(raw_signed[:libnacl.crypto_sign_BYTES])
        message = encoder.encode(raw_signed[libnacl.crypto_sign_BYTES:])
        signed = encoder.encode(raw_signed)

        return SignedMessage._from_parts(signature, message, signed)


class Signer:
    '''
    Used to sign messages with nacl digital signature
    '''

    def __init__(self, key=None):
        if key:
            if not isinstance(key, SigningKey):  # not key so seed to regenerate
                if len(key) == 32:
                    key = SigningKey(seed=key, encoder=encoding.RawEncoder)
                else:
                    key = SigningKey(seed=key, encoder=encoding.HexEncoder)
        else:
            key = SigningKey.generate()
        self.key = key
        self.keyhex = self.key.encode(encoding.HexEncoder)  # seed
        self.keyraw = self.key.encode(encoding.RawEncoder)  # seed
        self.verhex = self.key.verify_key.encode(encoding.HexEncoder)
        self.verraw = self.key.verify_key.encode(encoding.RawEncoder)

    def sign(self, msg):
        '''
        Sign the message
        '''
        return self.key.sign(msg)

    def signature(self, msg):
        '''
        Return only the signature string resulting from signing the message
        '''
        return self.key.sign(msg).signature


class Verifier:
    '''
    Used to verify messages with nacl digital signature
    '''

    def __init__(self, key=None):
        if key:
            if not isinstance(key, VerifyKey):
                if len(key) == 32:
                    key = VerifyKey(key, encoding.RawEncoder)
                else:
                    key = VerifyKey(key, encoding.HexEncoder)
        self.key = key
        if isinstance(self.key, VerifyKey):
            self.keyhex = self.key.encode(encoding.HexEncoder)
            self.keyraw = self.key.encode(encoding.RawEncoder)
        else:
            self.keyhex = ''
            self.keyraw = ''

    def verify(self, signature, msg):
        '''
        Verify the message
        '''
        if not self.key:
            return False
        try:
            self.key.verify(signature + msg)
        except ValueError:
            return False
        return True


class PublicKey(encoding.Encodable):
    """
    The public key counterpart to an Curve25519 :class:`PrivateKey`
    for encrypting messages.

    :param public_key: [:class:`bytes`] Encoded Curve25519 public key
    :param encoder: A class that is able to decode the `public_key`

    :cvar SIZE: The size that the public key is required to be
    """

    SIZE = libnacl.crypto_box_PUBLICKEYBYTES

    def __init__(self, public_key, encoder=encoding.RawEncoder):
        self._public_key = encoder.decode(public_key)

        if len(self._public_key) != self.SIZE:
            raise ValueError("The public key must be exactly %s bytes long" %
                             self.SIZE)

    def __bytes__(self):
        return self._public_key


class PrivateKey(encoding.Encodable):
    """
    Private key for decrypting messages using the Curve25519 algorithm.

    .. warning:: This **must** be protected and remain secret. Anyone who
        knows the value of your :class:`~PrivateKey` can decrypt
        any message encrypted by the corresponding
        :class:`~PublicKey`

    :param private_key: The private key used to decrypt messages
    :param encoder: The encoder class used to decode the given keys

    :cvar SIZE: The size that the private key is required to be
    """

    SIZE = libnacl.crypto_box_SECRETKEYBYTES

    def __init__(self, private_key, encoder=encoding.RawEncoder):
        # Decode the secret_key
        private_key = encoder.decode(private_key)

        # Verify that our seed is the proper size
        if len(private_key) != self.SIZE:
            raise ValueError(
                "The secret key must be exactly %d bytes long" % self.SIZE)

        raw_public_key = libnacl.crypto_scalarmult_base(private_key)

        self._private_key = private_key
        self.public_key = PublicKey(raw_public_key)

    def __bytes__(self):
        return self._private_key

    @classmethod
    def generate(cls):
        """
        Generates a random :class:`~PrivateKey` object

        :rtype: :class:`~PrivateKey`
        """
        return cls(libnacl.randombytes(PrivateKey.SIZE), encoder=encoding.RawEncoder)


class Box(encoding.Encodable):
    """
    The Box class boxes and unboxes messages between a pair of keys

    The ciphertexts generated by :class:`~Box` include a 16
    byte authenticator which is checked as part of the decryption. An invalid
    authenticator will cause the decrypt function to raise an exception. The
    authenticator is not a signature. Once you've decrypted the message you've
    demonstrated the ability to create arbitrary valid message, so messages you
    send are repudiable. For non-repudiable messages, sign them after
    encryption.

    :param private_key: :class:`~PrivateKey` used to encrypt and
        decrypt messages
    :param public_key: :class:`~PublicKey` used to encrypt and
        decrypt messages

    :cvar NONCE_SIZE: The size that the nonce is required to be.
    """

    NONCE_SIZE = libnacl.crypto_box_NONCEBYTES

    def __init__(self, private_key, public_key):
        if private_key and public_key:
            self._shared_key = libnacl.crypto_box_beforenm(
                public_key.encode(encoder=encoding.RawEncoder),
                private_key.encode(encoder=encoding.RawEncoder),
            )
        else:
            self._shared_key = None

    def __bytes__(self):
        return self._shared_key

    @classmethod
    def decode(cls, encoded, encoder=encoding.RawEncoder):
        # Create an empty box
        box = cls(None, None)

        # Assign our decoded value to the shared key of the box
        box._shared_key = encoder.decode(encoded)

        return box

    def encrypt(self, plaintext, nonce, encoder=encoding.RawEncoder):
        """
        Encrypts the plaintext message using the given `nonce` and returns
        the ciphertext encoded with the encoder.

        .. warning:: It is **VITALLY** important that the nonce is a nonce,
            i.e. it is a number used only once for any given key. If you fail
            to do this, you compromise the privacy of the messages encrypted.

        :param plaintext: [:class:`bytes`] The plaintext message to encrypt
        :param nonce: [:class:`bytes`] The nonce to use in the encryption
        :param encoder: The encoder to use to encode the ciphertext
        :rtype: [:class:`nacl.utils.EncryptedMessage`]
        """
        if len(nonce) != self.NONCE_SIZE:
            raise ValueError("The nonce must be exactly %s bytes long" %
                             self.NONCE_SIZE)

        ciphertext = libnacl.crypto_box_afternm(
            plaintext,
            nonce,
            self._shared_key,
        )

        encoded_nonce = encoder.encode(nonce)
        encoded_ciphertext = encoder.encode(ciphertext)

        return EncryptedMessage._from_parts(
            encoded_nonce,
            encoded_ciphertext,
            encoder.encode(nonce + ciphertext),
        )

    def decrypt(self, ciphertext, nonce=None, encoder=encoding.RawEncoder):
        """
        Decrypts the ciphertext using the given nonce and returns the
        plaintext message.

        :param ciphertext: [:class:`bytes`] The encrypted message to decrypt
        :param nonce: [:class:`bytes`] The nonce used when encrypting the
            ciphertext
        :param encoder: The encoder used to decode the ciphertext.
        :rtype: [:class:`bytes`]
        """
        # Decode our ciphertext
        ciphertext = encoder.decode(ciphertext)

        if nonce is None:
            # If we were given the nonce and ciphertext combined, split them.
            nonce = ciphertext[:self.NONCE_SIZE]
            ciphertext = ciphertext[self.NONCE_SIZE:]

        if len(nonce) != self.NONCE_SIZE:
            raise ValueError("The nonce must be exactly %s bytes long" %
                             self.NONCE_SIZE)

        plaintext = libnacl.crypto_box_open_afternm(
            ciphertext,
            nonce,
            self._shared_key,
        )

        return plaintext


class Publican:
    '''
    Container to manage remote nacl public key
        .key is the public key
    Intelligently converts hex encoded to object
    '''

    def __init__(self, key=None):
        if key:
            if not isinstance(key, PublicKey):
                if len(key) == 32:
                    key = PublicKey(key, encoding.RawEncoder)
                else:
                    key = PublicKey(key, encoding.HexEncoder)
        self.key = key
        if isinstance(self.key, PublicKey):
            self.keyhex = self.key.encode(encoding.HexEncoder)
            self.keyraw = self.key.encode(encoding.RawEncoder)
        else:
            self.keyhex = ''
            self.keyraw = ''


class Privateer:
    '''
    Container for local nacl key pair
        .key is the private key
    '''

    def __init__(self, key=None):
        if key:
            if not isinstance(key, PrivateKey):
                if len(key) == 32:
                    key = PrivateKey(key, encoding.RawEncoder)
                else:
                    key = PrivateKey(key, encoding.HexEncoder)
        else:
            key = PrivateKey.generate()
        self.key = key
        self.keyhex = self.key.encode(encoding.HexEncoder)
        self.keyraw = self.key.encode(encoding.RawEncoder)
        self.pubhex = self.key.public_key.encode(encoding.HexEncoder)
        self.pubraw = self.key.public_key.encode(encoding.RawEncoder)

    def nonce(self):
        '''
        Generate a safe nonce value (safe assuming only this method is used to
        create nonce values)
        '''
        return libnacl.randombytes(Box.NONCE_SIZE)

    def encrypt(self, msg, pubkey, enhex=False):
        '''
        Return duple of (cyphertext, nonce) resulting from encrypting the message
        using shared key generated from the .key and the pubkey
        If pubkey is hex encoded it is converted first
        If enhex is True then use HexEncoder otherwise use RawEncoder

        Intended for the owner of the passed in public key

        msg is string
        pub is Publican instance
        '''
        if not isinstance(pubkey, PublicKey):
            if len(pubkey) == 32:
                pubkey = PublicKey(pubkey, encoding.RawEncoder)
            else:
                pubkey = PublicKey(pubkey, encoding.HexEncoder)
        box = Box(self.key, pubkey)
        nonce = self.nonce()
        encoder = encoding.HexEncoder if enhex else encoding.RawEncoder
        encrypted = box.encrypt(msg, nonce, encoder)
        return (encrypted.ciphertext, encrypted.nonce)

    def decrypt(self, cipher, nonce, pubkey, dehex=False):
        '''
        Return decrypted msg contained in cypher using nonce and shared key
        generated from .key and pubkey.
        If pubkey is hex encoded it is converted first
        If dehex is True then use HexEncoder otherwise use RawEncoder

        Intended for the owner of .key

        cypher is string
        nonce is string
        pub is Publican instance
        '''
        if not isinstance(pubkey, PublicKey):
            if len(pubkey) == 32:
                pubkey = PublicKey(pubkey, encoding.RawEncoder)
            else:
                pubkey = PublicKey(pubkey, encoding.HexEncoder)
        box = Box(self.key, pubkey)
        decoder = encoding.HexEncoder if dehex else encoding.RawEncoder
        if dehex and len(nonce) != box.NONCE_SIZE:
            nonce = decoder.decode(nonce)
        return box.decrypt(cipher, nonce, decoder)
