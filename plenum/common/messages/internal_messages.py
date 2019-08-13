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

NeedMasterCatchup = NamedTuple('NeedMasterCatchup', [])

NeedBackupCatchup = NamedTuple('NeedBackupCatchup',
                               [('inst_id', int),
                                ('caught_up_till_3pc', tuple)])

CheckpointStabilized = NamedTuple('CheckpointStabilized',
                                  [('inst_id', int),
                                   ('last_stable_3pc', tuple)])

# by default view_no for StartViewChange is None meaning that we move to the next view
NeedViewChange = NamedTuple('StartViewChange',
                            [('view_no', int)])
NeedViewChange.__new__.__defaults__ = (None,) * len(NeedViewChange._fields)

ViewChangeStarted = NamedTuple('ViewChangeStarted',
                               [('view_no', int)])
ViewChangeFinished = NamedTuple('ViewChangeFinished',
                                [('view_no', int),
                                 ('view_changes', list),
                                 ('checkpoint', object),
                                 ('batches', list)])
ApplyNewView = NamedTuple('ApplyNewView',
                          [('view_no', int),
                           ('view_changes', list),
                           ('checkpoint', object),
                           ('batches', list)])
