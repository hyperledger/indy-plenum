import time
from typing import NamedTuple, Callable

from common.serializers.serialization import instance_change_db_serializer
from plenum.common.messages.node_messages import InstanceChange
from storage.helper import initKeyValueStorage
from stp_core.common.log import getlogger

logger = getlogger()


Vote = NamedTuple("Vote", [
    ("timestamp", int),
    ("reason", int)])


class InstanceChangeCash(dict):  # Dict[viewNo, Dict[nodeName, Vote]]

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

    def __init__(self, config, data_location, time_provider: Callable = time.perf_counter):
        self._config = config
        self._instance_change_db = self._load_instance_change_db(data_location)
        self._time_provider = time_provider
        self._cash = InstanceChangeCash()
        self._fill_cash_by_db()

    def add_vote(self, msg: InstanceChange, voter: str):
        view_no = msg.viewNo
        vote = Vote(timestamp=self._time_provider(),
                    reason=msg.reason)
        # add to cash
        self._cash.add(view_no, voter, vote)
        # add to db
        self._update_db_from_cash(view_no)

    def has_view(self, view_no: int) -> bool:
        self._update_votes(view_no)
        return view_no in self._cash

    def has_inst_chng_from(self, view_no: int, voter: str) -> bool:
        self._update_votes(view_no)
        return view_no in self._cash and voter in self._cash[view_no]

    def has_quorum(self, view_no: int, quorum: int) -> bool:
        self._update_votes(view_no)
        return view_no in self._cash and len(self._cash[view_no]) >= quorum

    def remove_view(self, view_to_remove: int):
        for view_no in sorted(self._cash.keys()):
            if view_no > view_to_remove:
                break
            del self._cash[view_no]
            self._instance_change_db.remove(str(view_no))

    def close_db(self):
        self._instance_change_db.close()

    def _update_votes(self, view_no: int):
        if self._config.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL <= 0 or view_no not in self._cash:
            return
        db_need_update = False
        for voter, vote in dict(self._cash[view_no]).items():
            now = self._time_provider()
            if vote.timestamp < now - self._config.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL:
                logger.info("Discard InstanceChange from {} for ViewNo {} "
                            "because it is out of date (was received {}sec "
                            "ago)".format(voter, view_no, int(now - vote.timestamp)))
                self._cash.remove_vote(view_no, voter)
                db_need_update = True
        if db_need_update:
            self._update_db_from_cash(view_no)

    def _load_instance_change_db(self, data_location):
        return initKeyValueStorage(self._config.nodeStatusStorage,
                                   data_location,
                                   self._config.nodeStatusDbName,
                                   db_config=self._config.db_node_status_db_config)

    def _update_db_from_cash(self, view_no):
        # value_as_dict = pp_key._asdict()
        serialized_value = \
            instance_change_db_serializer.serialize(self._cash.get(view_no, None))
        self._instance_change_db.put(str(view_no), serialized_value)

    def _fill_cash_by_db(self):
        for view_no, serialized_votes in self._instance_change_db.iterator(include_value=True):
            votes_as_dict = instance_change_db_serializer.deserialize(serialized_votes)
            for voter, vote_dict in votes_as_dict.items():
                vote = Vote(*vote_dict)
                if not isinstance(vote.timestamp, int):
                    raise TypeError("timestamp in Vote must be of int type")
                self._cash.add(int(view_no), voter, vote)
