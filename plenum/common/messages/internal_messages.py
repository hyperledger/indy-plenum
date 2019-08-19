from typing import NamedTuple, List, Any

from plenum.common.exceptions import SuspiciousNode

HookMessage = NamedTuple('HookMessage',
                         [('hook', int),
                          ('args', tuple)])

RequestPropagates = NamedTuple('RequestPropagates',
                               [('bad_requests', List)])

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
