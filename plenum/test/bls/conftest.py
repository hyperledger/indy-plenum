import pytest

from crypto.bls.bls_multi_signature import MultiSignatureValue, MultiSignature
from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.util import get_utc_epoch
from plenum.test.helper import generate_state_root

participants = ["Node1", "Node2", "Node3"]
signature = "somefakesignaturesomefakesignaturesomefakesignature"


@pytest.fixture()
def fake_state_root_hash():
    return generate_state_root()


@pytest.fixture()
def fake_multi_sig_value(fake_state_root_hash):
    return MultiSignatureValue(ledger_id=DOMAIN_LEDGER_ID,
                               state_root_hash=fake_state_root_hash,
                               pool_state_root_hash=generate_state_root(),
                               txn_root_hash=generate_state_root(),
                               timestamp=get_utc_epoch())


@pytest.fixture()
def fake_multi_sig(fake_multi_sig_value):
    return MultiSignature(
        signature=signature,
        participants=participants,
        value=fake_multi_sig_value)
