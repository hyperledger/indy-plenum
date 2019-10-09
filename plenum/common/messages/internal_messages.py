from typing import NamedTuple, List, Any, Optional

from plenum.common.exceptions import SuspiciousNode

# General recommendation for (naming) internal messages is follows:
# - internal messages are basically events, not commands
# - so in most cases from name it should be clear that "something happened"
# - in some cases message names may indicate that "something needs to happen"
# - avoid names that tell "do something", messages are not commands
# - avoid names that are just nouns, messages are not "things"

RequestPropagates = NamedTuple('RequestPropagates',
                               [('bad_requests', List)])

BackupSetupLastOrdered = NamedTuple('BackupSetupLastOrdered',
                                    [('inst_id', int)])

NeedMasterCatchup = NamedTuple('NeedMasterCatchup', [])

NeedBackupCatchup = NamedTuple('NeedBackupCatchup',
                               [('inst_id', int),
                                ('caught_up_till_3pc', tuple)])

CheckpointStabilized = NamedTuple('CheckpointStabilized',
                                  [('last_stable_3pc', tuple)])

RaisedSuspicion = NamedTuple('RaisedSuspicion',
                             [('inst_id', int),
                              ('ex', SuspiciousNode)])

PreSigVerification = NamedTuple('PreSigVerification',
                                [('cmsg', Any)])

MissingMessage = NamedTuple('MissingMessage',
                            [('msg_type', str),
                             ('key', tuple),
                             ('inst_id', int),
                             ('dst', List[str]),
                             ('stash_data', Optional[tuple])])

# by default view_no for StartViewChange is None meaning that we move to the next view
NeedViewChange = NamedTuple('NeedViewChange',
                            [('view_no', int)])
NeedViewChange.__new__.__defaults__ = (None,) * len(NeedViewChange._fields)

ViewChangeStarted = NamedTuple('ViewChangeStarted',
                               [('view_no', int)])

NewViewAccepted = NamedTuple('NewViewAccepted',
                             [('view_no', int),
                              ('view_changes', list),
                              ('checkpoint', object),
                              ('batches', list)])

NewViewCheckpointsApplied = NamedTuple('NewViewCheckpointsApplied',
                                       [('view_no', int),
                                        ('view_changes', list),
                                        ('checkpoint', object),
                                        ('batches', list)])

ReOrderedInNewView = NamedTuple('ReOrderedInNewView', [])
