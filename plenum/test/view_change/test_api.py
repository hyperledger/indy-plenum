import pytest

from plenum.server.quorums import Quorums
from plenum.test.testing_utils import FakeSomething


@pytest.fixture(scope='module')
def view_changer():
    config = FakeSomething(
        ViewChangeWindowSize=1,
        ForceViewChangeFreq=0
    )
    node = FakeSomething(
        name="fake node",
        ledger_ids=[0],
        config=config,
        quorums=Quorums(7)
    )
    return view_changer
