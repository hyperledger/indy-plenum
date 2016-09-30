from collections import deque


# TODO: Remove it if its not needed anymore
class StashingDeque:
    def __init__(self):
        super().__init__()
        self.main = deque()
        self.stashed = deque()
        self._stashEverything = False

    @property
    def stashEverything(self):
        return self._stashEverything

    @stashEverything.setter
    def stashEverything(self, value):
        self._stashEverything = value
        if not self._stashEverything:
            self.unstashAll()

    # manually stash an item
    def stash(self, item):
        self.stashed.append(item)

    # manually unstash all items
    def unstashAll(self):
        self.main.extend(self.stashed)

    def enterStashMode(self):
        self.stashEverything = True

    def leaveStashMode(self):
        self.stashEverything = False

    def _curDeq(self):
        return self.stashed if self._stashEverything else self.main

    def append(self, *args, **kwargs):
        return self._curDeq().append(*args, **kwargs)

    def appendleft(self, *args, **kwargs):
        return self._curDeq().appendLeft(*args, **kwargs)

    def clear(self, *args, **kwargs):
        self.main.clear(*args, **kwargs)
        self.stashed.clear(*args, **kwargs)

    def extend(self, *args, **kwargs):
        return self._curDeq().extend(*args, **kwargs)

    def extendleft(self, *args, **kwargs):
        return self._curDeq().extendLeft(*args, **kwargs)

    def pop(self, *args, **kwargs):
        return self.main.pop(*args, **kwargs)

    def popleft(self, *args, **kwargs):
        return self.main.popLeft(*args, **kwargs)

    def remove(self, value):
        return self.main.pop(value)

    def reverse(self):
        return self.main.reverse()

    def rotate(self, *args, **kwargs):
        return self.main.rotate(*args, **kwargs)

    def __contains__(self, *args, **kwargs):
        return self.main.__contains__(*args, **kwargs)

    def __delitem__(self, *args, **kwargs):
        return self.main.__delitem__(*args, **kwargs)

    def __getitem__(self, *args, **kwargs):
        return self.main.__getitem__(*args, **kwargs)

    def __iter__(self, *args, **kwargs):
        return self.main.__iter__(*args, **kwargs)

    def __len__(self, *args, **kwargs):
        return self.main.__len__(*args, **kwargs)

    def __reversed__(self):
        return self.main.__reversed__()

    def __setitem__(self, *args, **kwargs):
        return self.main.__setitem__(*args, **kwargs)

    def __sizeof__(self):
        return self.main.__sizeof__()
