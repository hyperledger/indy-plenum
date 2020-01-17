from collections import OrderedDict


class FreshnessState:
    def __init__(self, last_updated, last_marked_as_outdated) -> None:
        self.last_updated = last_updated
        self.last_marked_as_outdated = last_marked_as_outdated


class FreshnessChecker:
    def __init__(self,
                 freshness_timeout):
        self.freshness_timeout = freshness_timeout
        self._ledger_freshness = {}  # Dict[ledger_id -> FreshnessState]

    def register_ledger(self, ledger_id, initial_time):
        self._ledger_freshness[ledger_id] = FreshnessState(initial_time, initial_time)

    def check_freshness(self, ts):
        '''
        Get all ledger IDs for which
          A) not updated for more than Freshness Timeout
          B) hasn't been attempted to update (returned from this method) for more than Freshness Timeout
        Should be called whenever we need to decide if ledgers need to be updated.

        :param ts: the current time check the freshness against
        :return: an ordered dict of outdated ledgers sorted by the time from the last update (from oldest to newest)
         and then by ledger ID (in case of equal update time)
        '''
        outdated_ledgers = {}
        for ledger_id, freshness_state in self._ledger_freshness.items():
            if ts - freshness_state.last_updated <= self.freshness_timeout:
                continue
            if ts - freshness_state.last_marked_as_outdated <= self.freshness_timeout:
                continue

            outdated_ledgers[ledger_id] = ts - freshness_state.last_updated
            freshness_state.last_marked_as_outdated = ts

        # sort by last update time and then by ledger_id
        return OrderedDict(
            sorted(
                outdated_ledgers.items(),
                key=lambda item: (-item[1], item[0])
            )
        )

    def update_freshness(self, ledger_id, ts):
        '''
        Updates the time at which the ledger was updated.
        Should be called whenever a txn for the ledger is ordered.

        :param ledger_id: the ID of the ledgers a txn was ordered for
        :param ts: the current time
        :return: None
        '''
        if ledger_id in self._ledger_freshness:
            self._ledger_freshness[ledger_id].last_updated = ts

    def get_last_update_time(self):
        '''
        Gets the time at which each ledger was updated.
        Can be called at any time to get this information.

        :return: an ordered dict of outdated ledgers sorted by last update time (from old to new)
        and then by ledger ID (in case of equal update time)
        '''
        last_updated = {ledger_id: freshness_state.last_updated
                        for ledger_id, freshness_state in self._ledger_freshness.items()}
        return OrderedDict(
            sorted(
                last_updated.items(),
                key=lambda item: (item[1], item[0])
            )
        )
