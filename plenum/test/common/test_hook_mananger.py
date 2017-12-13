import pytest

from plenum.common.hook_manager import HookManager


def test_hook_registration():
    hook_ids = [1, 2, 4, 10]
    manager = HookManager(hook_ids=hook_ids)
    assert len(manager.hook_ids) == len(hook_ids)
    assert set(manager.hooks.keys()) == set(hook_ids)
    with pytest.raises(KeyError):
        manager.register_hook(9, lambda x, y: print(x, y))
    for i in hook_ids:
        assert len(manager.hooks[i]) == 0
        manager.register_hook(i, lambda x, y: print(+y+i))
        assert len(manager.hooks[i]) == 1


def test_hook_execution():
    hook_ids = [1, 2, 4, 10]
    manager = HookManager(hook_ids=hook_ids)

    resp1 = []
    resp2 = []

    def callable1(x, y):
        resp1.append(x + y)
        return x + y

    def callable2(x, y):
        resp2.append(x * y)
        return x * y

    # Register single callable for hook_id
    manager.register_hook(1, callable1)
    manager.register_hook(2, callable2)

    assert manager.execute_hook(1, 2, 3) == 5
    assert len(resp1) == 1
    assert resp1[0] == 5
    assert manager.execute_hook(2, 5, 9) == 45
    assert len(resp2) == 1
    assert resp2[0] == 45

    # Register multiple callables for hook_id
    manager.register_hook(4, callable1)
    manager.register_hook(4, callable2)

    # Result is execution of last callable
    assert manager.execute_hook(4, 100, 2) == 200
    assert resp1[-1] == 102
    assert resp2[-1] == 200

    # Result of execution for unknown hook_id is None
    assert manager.execute_hook(10, 2, 3) is None
