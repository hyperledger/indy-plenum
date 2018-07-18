def emulate_catchup(replica, ppSeqNo=100):
    if replica.isMaster:
        replica.caught_up_till_3pc((replica.viewNo, ppSeqNo))
    else:
        replica.catchup_clear_for_backup()

def emulate_select_primaries(replica):
    replica.primaryName = 'SomeAnotherNode'
    replica._setup_for_non_master_after_view_change(replica.viewNo)
