from typing import Optional

from plenum.common.messages.node_messages import PrePrepare
from plenum.common.types import f
from plenum.server.consensus.batch_id import BatchID


def replica_name_to_node_name(name: Optional[str]) -> Optional[str]:
    if name is None:
        return
    return name.rsplit(':', maxsplit=1)[0]


def get_original_viewno(pp):
    return pp.originalViewNo if f.ORIGINAL_VIEW_NO.nm in pp else pp.viewNo


def preprepare_to_batch_id(pre_prepare: PrePrepare) -> BatchID:
    pp_view_no = get_original_viewno(pre_prepare)
    return BatchID(pre_prepare.viewNo, pp_view_no, pre_prepare.ppSeqNo, pre_prepare.digest)
