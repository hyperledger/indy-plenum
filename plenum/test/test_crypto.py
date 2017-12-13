from binascii import hexlify

import pytest
from libnacl import randombytes, crypto_sign, crypto_sign_open
from libnacl.public import SecretKey, Box
from stp_core.crypto.util import ed25519SkToCurve25519, ed25519PkToCurve25519
from stp_core.crypto.nacl_wrappers import Signer, SigningKey, Verifier, \
    PrivateKey

pytestmark = pytest.mark.smoke


def testBoxing():
    msg = b'Hey there, a msg for you'

    # Generate the key pairs for Alice and bob, if secret keys already exist
    # they can be passed in, otherwise new keys will be automatically generated
    bob = SecretKey()

    alice = SecretKey()

    """
    Alice: aA (a is alices private key, A is Alice's public key)
    A = G*a
    Bob: bB
    B = G*b

    hash(a*B) == hash(b*A)     : hypothesis
    hash(a*G*b) == hash(b*G*a) : substitution
    hash(G*a*b) == hash(G*a*b) : commutative property of ECC math
    True!

    """

    # Create the boxes, this is an object which represents the combination of the
    # sender's secret key and the receiver's public key
    bob_box = Box(bob.sk, alice.pk)
    alice_box = Box(alice.sk, bob.pk)

    # Bob's box encrypts messages for Alice
    bob_ctxt = bob_box.encrypt(msg)
    # Alice's box decrypts messages from Bob
    bclear = alice_box.decrypt(bob_ctxt)
    # Alice can send encrypted messages which only Bob can decrypt
    alice_ctxt = alice_box.encrypt(msg)
    aclear = bob_box.decrypt(alice_ctxt)

    print(bob.for_json())
    print("bob's public key" + bob.hex_pk().hex())
    print("bob's secret key" + bob.hex_sk().hex())

    # tom = libnacl.public.PublicKey(b"sdsd")
    # tom.hex_pk()
    # tom.hex_sk()
    #
    #
    # susan = libnacl.public.SecretKey()
    # pk = random.getrandbits(256)


def testFullSigning():
    # stored securely/privately
    seed = randombytes(32)

    # generates key pair based on seed
    sk = SigningKey(seed=seed)

    # helper for signing
    signer = Signer(sk)

    # this is the public key used to verify signatures (securely shared
    # before-hand with recipient)
    verkey = signer.verhex

    # the message to be signed
    msg = b'1234'

    # the signature
    sig = signer.signature(msg)

    # helper for verification
    vr = Verifier(verkey)

    # verification
    isVerified = vr.verify(sig, msg)

    assert isVerified


def testSimpleSigning():
    sik = SigningKey(randombytes(32))
    sigkey = sik._signing_key
    assert len(sigkey) == 64  # why is a signing key 64 bytes and not 32?
    verkey = sik.verify_key._key
    assert len(verkey) == 32
    msg = b'1234'
    cmsg = crypto_sign(msg, sigkey)
    dmsg = crypto_sign_open(cmsg, verkey)
    assert msg == dmsg


def testSimpleSigningIsHasConsistentVerKey():
    seed = randombytes(32)

    sik = SigningKey(seed)
    sigkey = sik._signing_key
    verkey = sik.verify_key._key

    msg = b'1234'
    cmsg = crypto_sign(msg, sigkey)
    dmsg = crypto_sign_open(cmsg, verkey)
    cmsg2 = crypto_sign(msg, sigkey)
    dmsg2 = crypto_sign_open(cmsg2, verkey)
    assert msg == dmsg
    assert dmsg == dmsg2

    sik2 = SigningKey(seed)
    sigkey2 = sik2._signing_key
    verkey2 = sik2.verify_key._key

    assert sigkey2 == sigkey
    assert verkey2 == verkey


def testSimpleSigningWithSimpleKeys():
    print("signing with a simple (non-signing) key pair")
    sk = PrivateKey.generate()
    prikey = sk._private_key
    assert len(prikey) == 32
    pubkey = sk.public_key._public_key
    assert len(pubkey) == 32
    msg = b'1234'
    cmsg = crypto_sign(msg, prikey)


def testKeyConversionFromEd25519ToCurve25519():
    signer = Signer()
    sk = signer.keyraw
    vk = signer.verraw
    # Check when keys are passed as raw bytes
    secretKey = ed25519SkToCurve25519(sk)
    publicKey = ed25519PkToCurve25519(vk)
    assert PrivateKey(secretKey).public_key.__bytes__() == publicKey
    assert ed25519PkToCurve25519(vk, toHex=True) == \
        hexlify(PrivateKey(secretKey).public_key.__bytes__())

    # Check when keys are passed as hex
    secretKey = ed25519SkToCurve25519(hexlify(sk))
    publicKey = ed25519PkToCurve25519(hexlify(vk))
    assert PrivateKey(secretKey).public_key.__bytes__() == publicKey
