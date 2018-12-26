class FreshnessChecker():

    def __init__(self,
                 ledger_ids,
                 freshness_timeout,
                 initial_time):
        self.freshness_timeout = freshness_timeout
        self._ledger_freshness = {ledger_id: initial_time for ledger_id in ledger_ids}
        self._outdated_ledgers = []  # List[(ledger_id, outdated_time)]

    def check_frehsness(self, ts):
        self._outdated_ledgers = [(ledger_id, ts - last_ts)
                                  for ledger_id, last_ts in self._ledger_freshness.items()
                                  if ts - last_ts > self.freshness_timeout]
        self._outdated_ledgers.sort(key=lambda tup: tup[1])

    def update_freshness(self, ledger_id, ts):
        self._ledger_freshness[ledger_id] = ts

    def get_outdated_ledgers(self):
        return self._outdated_ledgers

    def pop_next_outdated_ledger(self):
        if not self._outdated_ledgers:
            return None, None
        return self._outdated_ledgers.pop()
