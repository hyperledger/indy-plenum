import pytest

from state.pruning_state import PruningState
from state.state import State
from storage.kv_in_memory import KeyValueStorageInMemory
from storage.kv_store_leveldb import KeyValueStorageLeveldb


@pytest.yield_fixture(params=['memory', 'leveldb'])
def state(request, tmpdir_factory) -> State:
    if request.param == 'memory':
        db = KeyValueStorageInMemory()
    if request.param == 'leveldb':
        db = KeyValueStorageLeveldb(tmpdir_factory.mktemp('').strpath,
                                    'some_db')
    state = PruningState(db)
    yield state
    state.close()


def test_state_proof_and_verification(state):
    state.set(b'k1', b'v1')
    state.set(b'k2', b'v2')
    state.set(b'k3', b'v3')
    state.set(b'k4', b'v4')

    p1 = state.generate_state_proof(b'k1')
    p2 = state.generate_state_proof(b'k2')
    p3 = state.generate_state_proof(b'k3')
    p4 = state.generate_state_proof(b'k4')

    # Verify correct proofs and values
    assert PruningState.verify_state_proof(state.headHash, b'k1', b'v1', p1)
    assert PruningState.verify_state_proof(state.headHash, b'k2', b'v2', p2)
    assert PruningState.verify_state_proof(state.headHash, b'k3', b'v3', p3)
    assert PruningState.verify_state_proof(state.headHash, b'k4', b'v4', p4)

    # Incorrect proof
    assert PruningState.verify_state_proof(state.headHash, b'k3', b'v3', p4)

    # Correct proof but incorrect value
    assert not PruningState.verify_state_proof(state.headHash, b'k2', b'v1', p2)
    assert not PruningState.verify_state_proof(state.headHash, b'k4', b'v2', p4)


def test_state_proof_and_verification_serialized(state):
    data = {k.encode(): v.encode() for k, v in
            [('k1', 'v1'), ('k2', 'v2'), ('k35', 'v55'), ('k70', 'v99')]}

    for k, v in data.items():
        state.set(k, v)

    proofs = {k: state.generate_state_proof(k, serialize=True) for k in data}

    for k, v in data.items():
        assert PruningState.verify_state_proof(state.headHash, k, v,
                                               proofs[k], serialized=True)


def test_state_proof_for_missing_data(state):
    p1 = state.generate_state_proof(b'k1')
    assert PruningState.verify_state_proof(state.headHash, b'k1', None, p1)
