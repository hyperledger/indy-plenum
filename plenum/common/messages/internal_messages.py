from typing import NamedTuple, List, Any

from plenum.common.exceptions import SuspiciousNode

# TODO: should be removed
ValidatorsChanged = NamedTuple('ValidatorsChange',
                               [('names', List[str])])

# TODO: should be removed
ParticipatingStatus = NamedTuple('LedgerParticipatingStatus',
                                 [('is_participating', bool)])

HookMessage = NamedTuple('HookMessage',
                         [('hook', int),
                          ('args', tuple)])

OutboxMessage = NamedTuple('OutboxMessage',
                           [('msg', Any)])

RequestPropagates = NamedTuple('RequestPropagates',
                               [('bad_requests', List)])

PrimariesBatchNeeded = NamedTuple('PrimariesBatchNeeded',
                                  [('pbn', bool)])

CurrentPrimaries = NamedTuple('CurrentPrimaries',
                              [('primaries', list)])

BackupSetupLastOrdered = NamedTuple('BackupSetupLastOrdered',
                                    [('inst_id', int)])

NeedMasterCatchup = NamedTuple('NeedMasterCatchup', [])

NeedBackupCatchup = NamedTuple('NeedBackupCatchup',
                               [('inst_id', int),
                                ('caught_up_till_3pc', tuple)])

CheckpointStabilized = NamedTuple('CheckpointStabilized',
                                  [('inst_id', int),
                                   ('last_stable_3pc', tuple)])

RaisedSuspicion = NamedTuple('RaisedSuspicion',
                             [('inst_id', int),
                              ('ex', SuspiciousNode)])

PreSigVerification = NamedTuple('PreSigVerification',
                                [('cmsg', Any)])
