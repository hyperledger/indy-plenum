import random

import pytest

from plenum.common.util import randomString
from state.pruning_state import PruningState
from storage.kv_in_memory import KeyValueStorageInMemory


@pytest.fixture()
def rand():
    return random.randint(0, 1000)


@pytest.fixture()
def state():
    return PruningState(KeyValueStorageInMemory())


@pytest.fixture()
def data(rand, state):
    d = []
    for i in range(rand):
        key = randomString()
        val = random.randint(1, 100)
        state.set(key.encode(), str(val).encode())
        d.append((key, val))
    return d


@pytest.fixture()
def sequence(data):
    seq = []
    num = len(data)
    for i in range(num):
        action = random.randint(0, 1)
        if action:
            to_remove = random.randint(0, len(data) - 1)
            key, val = data.pop(to_remove)
        else:
            key = randomString()
            val = random.randint(1, 100)
            data.append((key, val))
        seq.append((action, key, val))
    return seq, data


@pytest.mark.parametrize('execution_number', range(10))
def test_removal_from_trie(sequence, state, execution_number):
    sequence, data = sequence
    for action, key, val in sequence:
        if action:
            assert val == int(state.get(key.encode(), isCommitted=False).decode())
            state.remove(key.encode())
        else:
            state.set(key.encode(), str(val).encode())
    for key, val in data:
        assert val == int(state.get(key.encode(), isCommitted=False).decode())
