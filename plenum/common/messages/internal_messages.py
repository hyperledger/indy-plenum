from typing import NamedTuple, List


ValidatorsChanged = NamedTuple('ValidatorsChange',
                               [('names', List[str])])

ViewChangeInProgress = NamedTuple('StartViewChange',
                                  [('in_progress', bool)])

LedgerSyncStatus = NamedTuple('LedgerSyncStatus',
                              [('is_synced', bool)])

ParticipatingStatus = NamedTuple('LedgerParticipatingStatus',
                                 [('is_participating', bool)])
