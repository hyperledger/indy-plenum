import pytest

from plenum.common.util import randomString
from storage.optimistic_kv_store import OptimisticKVStore


@pytest.fixture()
def optimistic_store(parametrised_storage) -> OptimisticKVStore:
    store = OptimisticKVStore(parametrised_storage)
    return store


def gen_data(num):
    return {randomString(32).encode(): randomString(100).encode()
            for _ in range(num)}


def test_set_get_reject_keys_basic(optimistic_store):
    # Set some keys, check their values, commit and check values again,
    # reject and check values again
    num_batches = 5
    num_keys_per_batch = 5
    batch_keys = []
    data = gen_data(num_batches * num_keys_per_batch)
    i = 0
    for k, v in data.items():
        if i % num_batches == 0:
            batch_keys.append([])
        batch_keys[-1].append((k, v))
        i += 1

    assert len(batch_keys) == num_batches

    def chk_keys(batch, uncommitted=True, committed=False):
        for k, v in batch:
            if uncommitted:
                assert optimistic_store.get(k, is_committed=False) == v
            else:
                with pytest.raises(KeyError):
                    optimistic_store.get(k, is_committed=False)

            if committed:
                assert optimistic_store.get(k, is_committed=True) == v
            else:
                with pytest.raises(KeyError):
                    optimistic_store.get(k, is_committed=True)

    for i, batch in enumerate(batch_keys):
        assert not optimistic_store.current_batch_ops

        for k, v in batch:
            optimistic_store.set(k, v, is_committed=False)

        assert optimistic_store.current_batch_ops
        old_len = len(optimistic_store.un_committed)
        chk_keys(batch)

        optimistic_store.create_batch_from_current(i)

        assert not optimistic_store.current_batch_ops
        assert len(optimistic_store.un_committed) == old_len + 1
        chk_keys(batch)

        if i < 3:
            # Test uncommitted and committed set and get
            assert optimistic_store.commit_batch() == i
            assert len(optimistic_store.un_committed) == old_len
            chk_keys(batch, committed=True)
        else:
            # Test reject
            optimistic_store.reject_batch()
            assert len(optimistic_store.un_committed) == old_len
            chk_keys(batch, uncommitted=False, committed=False)


def test_set_get_reject_same_keys(optimistic_store):
    # Set some keys, commit them, check their values, set new values for them
    # in new batch, check committed and non committed values
    num_keys = 10
    keys = [randomString(32).encode() for _ in range(num_keys)]
    vals_1 = {k: randomString(100).encode() for k in keys}
    vals_2 = {k: randomString(100).encode() for k in keys}
    vals_3 = {k: randomString(100).encode() for k in keys}

    for k, v in vals_1.items():
        optimistic_store.set(k, v, is_committed=False)

    optimistic_store.create_batch_from_current(randomString(10))
    optimistic_store.commit_batch()

    for k, v in vals_1.items():
        assert optimistic_store.get(k, is_committed=False) == v
        assert optimistic_store.get(k, is_committed=True) == v

    for k, v in vals_2.items():
        optimistic_store.set(k, v, is_committed=False)

    optimistic_store.create_batch_from_current(randomString(10))

    for k, v in vals_2.items():
        assert optimistic_store.get(k, is_committed=False) == v
        assert optimistic_store.get(k, is_committed=True) == vals_1[k]

    # More than 1 uncommitted batch exists containing the same keys but value
    # of a key is equal to the value in last batch
    for k, v in vals_3.items():
        optimistic_store.set(k, v, is_committed=False)

    optimistic_store.create_batch_from_current(randomString(10))

    for k, v in vals_3.items():
        assert optimistic_store.get(k, is_committed=False) != vals_1[k]
        assert optimistic_store.get(k, is_committed=False) != vals_2[k]
        assert optimistic_store.get(k, is_committed=False) == v
