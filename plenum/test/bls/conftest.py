import os

import pytest
from plenum.bls.bls import BlsFactoryCharm


@pytest.fixture()
def tempdir(tmpdir_factory):
    return tmpdir_factory.mktemp('').strpath

@pytest.fixture()
def bls_serializer(tempdir):
    os.mkdir(os.path.join(tempdir, 'Node1'))
    factory = BlsFactoryCharm(tempdir, 'Node1')
    return factory.create_serializer()

