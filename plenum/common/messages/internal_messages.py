from typing import NamedTuple, List, Any

from plenum.common.messages.node_messages import CheckpointState, PrePrepare

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

AddToCheckpointMsg = NamedTuple('AddToCheckpointMsg',
                                [('inst_id', int),
                                 ('pp_seq_no', int),
                                 ('digest', str),
                                 ('ledger_id', int),
                                 ('view_no', int)])

RemoveStashedCheckpoints = NamedTuple('RemoveStashedCheckpoints',
                                      [('inst_id', int),
                                       ('till_3pc_key', tuple)])

RequestPropagates = NamedTuple('RequestPropagates',
                               [('bad_requests', List)])

NodeModeMsg = NamedTuple('NodeModeMsg',
                         [('mode', int)])

PrimariesBatchNeeded = NamedTuple('PrimariesBatchNeeded',
                                  [('pbn', bool)])

CurrentPrimaries = NamedTuple('CurrentPrimaries',
                              [('primaries', list)])

RevertUnorderedBatches = NamedTuple('RevertUnorderedBatches', [('inst_id', int)])

OnViewChangeStartMsg = NamedTuple('OnViewChangeStartMsg',
                                  [('inst_id', int)])

OnCatchupFinishedMsg = NamedTuple('OnCatchupFinished',
                                  [('inst_id', int),
                                   ('last_caught_up_3PC', tuple),
                                   ('master_last_ordered_3PC', tuple)])
