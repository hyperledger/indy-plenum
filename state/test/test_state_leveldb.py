import copy
import os

import pytest
from state.kv.kv_store_leveldb import KeyValueStorageLeveldb
from state.pruning_state import PruningState
from state.state import State
from state.trie.pruning_trie import BLANK_NODE, BLANK_ROOT

i = 0

#TODO: combine with in-memory tests


@pytest.yield_fixture(scope="function")
def state(tempdir) -> State:
    global i
    state = PruningState(
        KeyValueStorageLeveldb(os.path.join(tempdir, 'kv{}'.format(i))))
    yield state
    state.close()


@pytest.yield_fixture(scope="function")
def state2(tempdir) -> State:
    global i
    state = PruningState(
        KeyValueStorageLeveldb(os.path.join(tempdir, 'kv2{}'.format(i))))
    yield state
    state.close()


def test_set(state):
    state.set(b'k1', b'v1')
    state.commit(state.headHash)
    assert b'v1' == state.get(b'k1')

    state.set(b'k2', b'v2')
    state.commit(state.headHash)
    assert b'v2' == state.get(b'k2')


def test_set_same_key(state):
    state.set(b'k1', b'v1')
    state.commit(state.headHash)
    assert b'v1' == state.get(b'k1')

    state.set(b'k1', b'v2')
    state.commit(state.headHash)
    assert b'v2' == state.get(b'k1')


def test_get(state):
    state.set(b'k1', b'v1')
    assert b'v1' == state.get(b'k1', isCommitted=False)
    assert None == state.get(b'k1', isCommitted=True)

    state.commit(state.headHash)
    assert b'v1' == state.get(b'k1', isCommitted=False)
    assert b'v1' == state.get(b'k1', isCommitted=True)

    state.set(b'k2', b'v2')
    assert b'v2' == state.get(b'k2', isCommitted=False)
    assert None == state.get(b'k2', isCommitted=True)
    assert b'v1' == state.get(b'k1', isCommitted=True)

    state.set(b'k1', b'v3')
    assert b'v3' == state.get(b'k1', isCommitted=False)
    assert b'v1' == state.get(b'k1', isCommitted=True)


def test_remove_uncommitted(state):
    state.set(b'k1', b'v1')
    assert b'v1' == state.get(b'k1', isCommitted=False)
    assert None == state.get(b'k1', isCommitted=True)

    state.remove(b'k1')
    assert None == state.get(b'k1', isCommitted=False)
    assert None == state.get(b'k1', isCommitted=True)


def test_remove_committed(state):
    state.set(b'k1', b'v1')
    state.commit(state.headHash)
    assert b'v1' == state.get(b'k1', isCommitted=False)
    assert  b'v1' == state.get(b'k1', isCommitted=True)

    state.remove(b'k1')
    # do not remove committed
    assert None == state.get(b'k1', isCommitted=False)
    assert b'v1' == state.get(b'k1', isCommitted=True)


def test_revert_to_last_committed_head(state):
    state.set(b'k1', b'v1')
    state.commit(state.headHash)
    state.set(b'k1', b'v2')
    assert b'v2' == state.get(b'k1', isCommitted=False)
    assert  b'v1' == state.get(b'k1', isCommitted=True)

    state.revertToHead(state.committedHead)
    assert b'v1' == state.get(b'k1', isCommitted=False)
    assert b'v1' == state.get(b'k1', isCommitted=True)


def test_revert_to_old_head(state):
    state.set(b'k1', b'v1')
    state.commit(state.headHash)
    head1 = state.committedHeadHash
    state.set(b'k1', b'v2')
    state.commit(state.headHash)
    state.set(b'k1', b'v3')
    state.commit(state.headHash)
    assert b'v3' == state.get(b'k1', isCommitted=False)
    assert  b'v3' == state.get(b'k1', isCommitted=True)

    state.revertToHead(head1)
    assert b'v1' == state.get(b'k1', isCommitted=False)
    # do not revert committed
    assert b'v3' == state.get(b'k1', isCommitted=True)


def test_head_initially(state):
    assert BLANK_NODE == state.head
    assert BLANK_ROOT == state.headHash


def test_state_head_after_updates(state, state2):
    state.set(b'k1', b'v1')
    state.set(b'k2', b'v2')
    state.set(b'k1', b'v1a')
    state.set(b'k3', b'v3')
    state.remove(b'k2')

    state2.set(b'k1', b'v1a')
    state2.set(b'k3', b'v3')

    assert state.headHash == state2.headHash
    assert state.head == state2.head


def test_committed_head_initially(state):
    assert BLANK_NODE == state.committedHead
    assert BLANK_ROOT == state.committedHeadHash


def test_committed_state_head_after_updates(state, state2):
    state.set(b'k1', b'v1')
    state.set(b'k2', b'v2')
    state.commit(state.headHash)
    state.set(b'k1', b'v1a')
    state.set(b'k3', b'v3')

    state2.set(b'k1', b'v1')
    state2.set(b'k2', b'v2')
    state2.commit(state2.headHash)

    assert state.committedHead == state2.committedHead
    assert state.committedHeadHash == state2.committedHeadHash


def test_commit_current(state):
    state.set(b'k1', b'v1')
    state.set(b'k2', b'v2')
    head = state.head
    headHash = state.headHash
    state.commit()

    assert head == state.committedHead
    assert headHash == state.committedHeadHash


def test_commit_multiple_times(state):
    state.set(b'k1', b'v1')
    state.set(b'k2', b'v2')
    head = state.head
    headHash = state.headHash
    state.commit()
    state.commit()
    state.commit()
    state.commit()
    state.commit()

    assert head == state.committedHead
    assert headHash == state.committedHeadHash


def test_commit_to_current_head_hash(state):
    state.set(b'k1', b'v1')
    state.set(b'k2', b'v2')
    head = state.head
    headHash = state.headHash
    state.commit(headHash)

    assert head == state.committedHead
    assert headHash == state.committedHeadHash


def test_commit_to_old_head_hash(state):
    state.set(b'k1', b'v1')
    state.set(b'k2', b'v2')
    headHash = state.headHash
    state.set(b'k3', b'v3')
    state.set(b'k4', b'v4')
    state.commit(headHash)

    assert headHash == state.committedHeadHash


def test_commit_to_current_head(state):
    state.set(b'k1', b'v1')
    state.set(b'k2', b'v2')
    head = state.head
    headHash = state.headHash
    state.commit(rootNode=head)

    assert head == state.committedHead
    assert headHash == state.committedHeadHash


def test_commit_to_old_head(state):
    state.set(b'k1', b'v1')
    state.set(b'k2', b'v2')
    head = copy.deepcopy(state.head)
    headHash = state.headHash
    state.set(b'k3', b'v3')
    state.set(b'k4', b'v4')
    state.commit(rootNode=head)

    assert head == state.committedHead
    assert headHash == state.committedHeadHash

def testStateData(state):
    state.set(b'k1', b'v1')
    state.set(b'k2', b'v2')
    state.set(b'k3', b'v3')

    data = {k: v for k, v in state.as_dict.items()}
    assert data == {b'k1': b'v1', b'k2': b'v2', b'k3': b'v3'}
