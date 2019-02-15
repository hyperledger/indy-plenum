import time
from typing import NamedTuple, Callable

from common.serializers.serialization import instance_change_db_serializer
from plenum.common.messages.node_messages import InstanceChange
from storage.helper import initKeyValueStorage
from storage.kv_store import KeyValueStorage
from stp_core.common.log import getlogger

logger = getlogger()


Vote = NamedTuple("Vote", [
    ("timestamp", float),
    ("reason", int)])


class InstanceChangeCache(dict):  # Dict[viewNo, Dict[nodeName, Vote]]

    def add(self, view_no, voter, vote: Vote):
        self.setdefault(view_no, {})
        self[view_no][voter] = vote

    def remove_vote(self, view_no, voter):
        if view_no not in self or voter not in self[view_no]:
            return
        del self[view_no][voter]
        if not self[view_no]:
            del self[view_no]


class InstanceChangeProvider:

    def __init__(self, outdated_ic_interval: int = 0,
                 instance_change_db: KeyValueStorage = None,
                 time_provider: Callable = time.perf_counter):
        self._outdated_ic_interval = outdated_ic_interval
        self._cache = InstanceChangeCache()
        self._time_provider = time_provider
        self._instance_change_db = instance_change_db
        self._fill_cache_by_db()

    def add_vote(self, msg: InstanceChange, voter: str):
        view_no = msg.viewNo
        vote = Vote(timestamp=self._time_provider(),
                    reason=msg.reason)
        # add to cache
        self._cache.add(view_no, voter, vote)
        # add to db
        self._update_db_from_cache(view_no)

    def has_view(self, view_no: int) -> bool:
        self._update_votes(view_no)
        return view_no in self._cache

    def has_inst_chng_from(self, view_no: int, voter: str) -> bool:
        self._update_votes(view_no)
        return view_no in self._cache and voter in self._cache[view_no]

    def has_quorum(self, view_no: int, quorum: int) -> bool:
        self._update_votes(view_no)
        return view_no in self._cache and len(self._cache[view_no]) >= quorum

    def remove_view(self, view_to_remove: int):
        for view_no in sorted(self._cache.keys()):
            if view_no > view_to_remove:
                break
            del self._cache[view_no]
            if self._instance_change_db:
                self._instance_change_db.remove(str(view_no))

    def items(self):
        return dict(self._cache).items()

    def _update_votes(self, view_no: int):
        if self._outdated_ic_interval <= 0 or view_no not in self._cache:
            return
        db_need_update = False
        for voter, vote in dict(self._cache[view_no]).items():
            now = self._time_provider()
            if vote.timestamp < now - self._outdated_ic_interval:
                logger.info("Discard InstanceChange from {} for ViewNo {} "
                            "because it is out of date (was received {}sec "
                            "ago)".format(voter, view_no, int(now - vote.timestamp)))
                self._cache.remove_vote(view_no, voter)
                db_need_update = True
        if db_need_update:
            self._update_db_from_cache(view_no)

    def _update_db_from_cache(self, view_no):
        if not self._instance_change_db:
            return
        # value_as_dict = pp_key._asdict()
        serialized_value = \
            instance_change_db_serializer.serialize(self._cache.get(view_no, None))
        self._instance_change_db.put(str(view_no), serialized_value)

    def _fill_cache_by_db(self):
        if not self._instance_change_db:
            return
        for view_no, serialized_votes in self._instance_change_db.iterator(include_value=True):
            votes_as_dict = instance_change_db_serializer.deserialize(serialized_votes)
            for voter, vote_dict in votes_as_dict.items():
                vote = Vote(*vote_dict)
                if not isinstance(vote.timestamp, (float, int)):
                    raise TypeError("timestamp in Vote must be of float type")
                if not isinstance(vote.reason, int):
                    raise TypeError("reason in Vote must be of int type")
                self._cache.add(int(view_no), voter, vote)
