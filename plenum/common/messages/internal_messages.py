from typing import NamedTuple, List, Any

ValidatorsChanged = NamedTuple('ValidatorsChange',
                               [('names', List[str])])

LegacyViewChangeStatusUpdate = NamedTuple('StartViewChange',
                                          [('in_progress', bool)])

ParticipatingStatus = NamedTuple('LedgerParticipatingStatus',
                                 [('is_participating', bool)])

HookMessage = NamedTuple('HookMessage',
                         [('hook', int),
                          ('args', tuple)])

OutboxMessage = NamedTuple('OutboxMessage',
                           [('msg', Any)])

