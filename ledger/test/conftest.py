import pytest


@pytest.fixture(scope='module')
def tdir(tmpdir_factory):
    return tmpdir_factory.mktemp('').strpath


@pytest.fixture(scope='function')
def tempdir(tmpdir_factory):
    return tmpdir_factory.mktemp('').strpath
