import random

import pytest

from plenum.common.util import randomString
from state.pruning_state import PruningState
from storage.kv_in_memory import KeyValueStorageInMemory


rand = 1000


@pytest.fixture()
def state():
    return PruningState(KeyValueStorageInMemory())


@pytest.fixture()
def data(state):
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
        action = random.randint(0, 2)
        if action == 1:
            to_remove = random.randint(0, len(data) - 1)
            key, val = data.pop(to_remove)
            seq.append((action, key, val))
        elif action == 0:
            key = randomString()
            val = random.randint(1, 100)
            data.append((key, val))
            seq.append((action, key, val))
        elif action == 2:
            seq.append((action, None, None))
    return seq, data


@pytest.mark.parametrize('execution_number', range(10))
def test_removal_from_trie(sequence, state, execution_number):
    sequence, data = sequence
    removedKeys = []
    for action, key, val in sequence:
        if action == 1:
            assert val == int(state.get(key.encode(), isCommitted=False).decode())
            state.remove(key.encode())
            removedKeys.append(key)
        elif action == 0:
            state.set(key.encode(), str(val).encode())
        elif action == 2:
            state.commit()
    for key, val in data:
        assert val == int(state.get(key.encode(), isCommitted=False).decode())
    for key in removedKeys:
        assert state.get(key.encode(), isCommitted=False) is None
