from functools import partial

import pytest

from plenum.common.exceptions import RequestNackedException
from plenum.test.helper import sdk_send_random_and_check

whitelist = ['discarding message']


class TestVerifier:
    @staticmethod
    def verify(operation):
        assert operation['amount'] <= 100, 'amount too high'


@pytest.fixture(scope="module")
def restrictiveVerifier(txnPoolNodeSet):
    for n in txnPoolNodeSet:
        n.opVerifiers = [TestVerifier()]


@pytest.mark.skip(reason="old style plugin")
def testRequestFullRoundTrip(restrictiveVerifier,
                             sdk_pool_handle,
                             sdk_wallet_client,
                             looper,
                             txnPoolNodeSet):
    update = {'reason': 'client request invalid: InvalidClientRequest() '
                        '[caused by amount too high\nassert 999 <= 100]'}
    with pytest.raises(RequestNackedException) as e:
        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_client, 1)
    assert 'client request invalid: InvalidClientRequest() '
    '[caused by amount too high\nassert 999 <= 100]' in \
    e._excinfo[1].args[0]
