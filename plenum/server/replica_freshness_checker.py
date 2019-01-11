from collections import OrderedDict


class FreshnessState():

    def __init__(self, last_updated, last_marked_as_outdated) -> None:
        self.last_updated = last_updated
        self.last_marked_as_outdated = last_marked_as_outdated


class FreshnessChecker():

    def __init__(self,
                 freshness_timeout):
        self.freshness_timeout = freshness_timeout
        self._ledger_freshness = {}  # Dict[ledger_id -> FreshnessState]
        self._outdated_ledgers = OrderedDict()

    def register_ledger(self, ledger_id, initial_time):
        self._ledger_freshness[ledger_id] = FreshnessState(initial_time, initial_time)

    def check_freshness(self, ts):
        '''
        Get all ledger IDs for which
          A) not updated for more than Freshness Timeout
          B) hasn't been checked for more than than Freshness Timeout

        :param ts: the current time check the freshness against
        :return: None
        '''
        for ledger_id, freshness_state in self._ledger_freshness.items():
            if ts - freshness_state.last_updated <= self.freshness_timeout:
                continue
            if ts - freshness_state.last_marked_as_outdated <= self.freshness_timeout:
                continue

            self._outdated_ledgers[ledger_id] = ts - freshness_state.last_updated
            freshness_state.last_marked_as_outdated = ts

        # sort by last update time and then by ledger_id
        self._outdated_ledgers = OrderedDict(
            sorted(
                self._outdated_ledgers.items(),
                key=lambda item: (item[1], -item[0])
            )
        )

    def update_freshness(self, ledger_id, ts):
        if ledger_id in self._ledger_freshness:
            self._ledger_freshness[ledger_id].last_updated = ts

    def get_outdated_ledgers_count(self):
        return len(self._outdated_ledgers)

    def pop_next_outdated_ledger(self):
        if not self._outdated_ledgers:
            return None
        return self._outdated_ledgers.popitem()
