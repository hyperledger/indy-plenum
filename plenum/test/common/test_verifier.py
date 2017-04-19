from plenum.common.verifier import DidVerifier

SAMPLE_ABBR_VERKEY = '~8zH9ZSyZTFPGJ4ZPL5Rvxx'
SAMPLE_IDENTIFIER = '99BgFBg35BehzfSADV5nM4'
EXPECTED_VERKEY = '5SMfqc4NGeQM21NMx3cB9sqop6KCFFC1TqoGKGptdock'


def test_create_verifier():
    verifier = DidVerifier(SAMPLE_ABBR_VERKEY, identifier=SAMPLE_IDENTIFIER)
    assert verifier.verkey == EXPECTED_VERKEY
