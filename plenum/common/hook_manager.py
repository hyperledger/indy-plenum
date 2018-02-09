from typing import Callable, Dict, List

from common.error import error


class HookManager:
    def __init__(self, hook_ids):
        self.hook_ids = hook_ids
        self.hooks = {}     # type: Dict[int, List[Callable]]
        self.init_hooks()

    def init_hooks(self):
        for hook_id in self.hook_ids:
            self.hooks[hook_id] = []

    def register_hook(self, hook_id, hook: Callable):
        if hook_id not in self.hooks:
            error('Unknown hook id', KeyError)
        self.hooks[hook_id].append(hook)

    def execute_hook(self, hook_id, *args, **kwargs):
        # Returns the last executed hook's return value
        r = None
        for hook in self.hooks.get(hook_id, []):
            r = hook(*args, **kwargs)
        return r
