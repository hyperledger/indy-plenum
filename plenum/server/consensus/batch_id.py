
# `view_no` is a view no is the current view_no, but `pp_view_no` is a view no when the given PrePrepare has been
# initially created and applied

# it's critical to keep the original view no to correctly create audit ledger transaction
# (since PrePrepare's view no is present there)

# An example when `view_no` != `pp_view_no`, is when view change didn't finish at first round
# (next primary is unavailable for example)
from typing import NamedTuple

BatchID = NamedTuple('BatchID', [('view_no', int), ('pp_view_no', int), ('pp_seq_no', int), ('pp_digest', str)])
