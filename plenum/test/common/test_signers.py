import base58

from plenum.common.signer_did import DidSigner
from plenum.common.signer_simple import SimpleSigner
from plenum.common.util import friendlyToRaw


def test_signer_compatable():
    seed = b'Falcon00000000000000000000000000'
    s_signer = SimpleSigner(seed=seed)
    did_signer = DidSigner(seed=seed)

    assert friendlyToRaw(s_signer.identifier).startswith(
        friendlyToRaw(did_signer.identifier))

    if did_signer.abbreviated:
        verkey = friendlyToRaw(did_signer.identifier)
        verkey += friendlyToRaw(did_signer.verkey[1:])
    else:
        verkey = friendlyToRaw(did_signer.verkey)
    assert friendlyToRaw(s_signer.verkey) == verkey
    assert friendlyToRaw(s_signer.verkey) == friendlyToRaw(
        did_signer.full_verkey)


def test_compare_identities():
    cryptonym = 'BPtrqHo3WyjmTNpVchEhWxp3qfDdssdFUNoM8kmKoEWw'
    did_id = 'L5AD5g65TDQr1PPHHRoiGf'
    did_verkey = 'Bf9Z1tKWpcJAvKJVhZhvVZ'

    did_to_cryptonym = base58.b58encode(
        base58.b58decode(did_id) + base58.b58decode(did_verkey))
    assert cryptonym == did_to_cryptonym
