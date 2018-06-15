import pytest

from plenum.common.exceptions import InvalidKey
from plenum.common.verifier import DidVerifier

SAMPLE_ABBR_VERKEY = '~8zH9ZSyZTFPGJ4ZPL5Rvxx'
SAMPLE_IDENTIFIER = '99BgFBg35BehzfSADV5nM4'
EXPECTED_VERKEY = '5SMfqc4NGeQM21NMx3cB9sqop6KCFFC1TqoGKGptdock'
ODD_LENGTH_VERKEY = 'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'


def test_DidVerifier_init_invalid_args():
    for verkey in (None, ''):
        with pytest.raises(ValueError) as excinfo:
            DidVerifier(verkey, identifier=SAMPLE_IDENTIFIER)
        assert "'verkey' should be a non-empty string" == str(excinfo.value)


def test_create_verifier():
    verifier = DidVerifier(SAMPLE_ABBR_VERKEY, identifier=SAMPLE_IDENTIFIER)
    assert verifier.verkey == EXPECTED_VERKEY


def test_create_verifier_with_odd_length_verkey():
    with pytest.raises(
            InvalidKey,
            message="invalid verkey {} accepted".format(
                ODD_LENGTH_VERKEY)) as excinfo:
        verifier = DidVerifier(ODD_LENGTH_VERKEY)
    excinfo.match(r'verkey {}'.format(ODD_LENGTH_VERKEY))
