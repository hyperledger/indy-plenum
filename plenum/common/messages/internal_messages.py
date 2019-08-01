from typing import NamedTuple, List, Any

from plenum.common.messages.node_messages import CheckpointState

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

DoCheckpointMessage = NamedTuple('DoCheckpoinitMessage',
                                 [('state', CheckpointState),
                                  ('start_no', int),
                                  ('end_no', int),
                                  ('ledger_id', int),
                                  ('view_no', int)])

RemoveStashedCheckpoints = NamedTuple('RemoveStashedCheckpoints',
                                      [('start_no', int),
                                       ('end_no', int),
                                       ('view_no', int),
                                       ('all', bool)])

RequestPropagates = NamedTuple('RequestPropagates',
                               [('bad_requests', List)])

StartMasterCatchup = NamedTuple('StartMasterCatchup', [])

StartBackupCatchup = NamedTuple('StartBackupCatchup',
                                [('caught_up_till_3pc', tuple)])

Cleanup = NamedTuple('Cleanup',
                     [('cleanup_till_3pc', tuple)])
