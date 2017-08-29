import logging
import os
import pytest

logger = logging.getLogger()


basePath = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture(scope='module')
def tdir(tmpdir_factory):
    return tmpdir_factory.mktemp('').strpath


@pytest.fixture(scope='function')
def tdir_for_func(tmpdir_factory):
    return tmpdir_factory.mktemp('').strpath
