from typing import NamedTuple, Optional

from common.serializers.serialization import node_status_db_serializer
from plenum.common.constants import LAST_SENT_PRE_PREPARE
from stp_core.common.log import getlogger

logger = getlogger()

PrePrepareKey = NamedTuple("PrePrepareKey",
                           [("inst_id", int),
                            ("view_no", int),
                            ("pp_seq_no", int)])


class LastSentPpStoreHelper:

    def __init__(self, node):
        self.node = node

    def store_last_sent_pp_seq_no(self, inst_id: int, pp_seq_no: int):
        self._save_last_sent_pp_key(PrePrepareKey(inst_id=inst_id,
                                                  view_no=self.node.viewNo,
                                                  pp_seq_no=pp_seq_no))

    def erase_last_sent_pp_seq_no(self):
        logger.info("{} erasing stored lastSentPrePrepare".format(self.node))
        if LAST_SENT_PRE_PREPARE in self.node.nodeStatusDB:
            self.node.nodeStatusDB.remove(LAST_SENT_PRE_PREPARE)

    def try_restore_last_sent_pp_seq_no(self):
        logger.info("{} trying to restore lastPrePrepareSeqNo".format(self.node))

        try:
            last_sent_pp_key = self._load_last_sent_pp_key()
        except Exception as e:
            logger.warning("{} cannot unpack inst_id, view_no, pp_seq_no "
                           "from stored lastSentPrePrepare: {}"
                           .format(self.node, e))
            return False

        if last_sent_pp_key is None:
            return False

        if self._can_restore_last_sent_pp_seq_no(last_sent_pp_key):
            self._restore_last_sent_pp_seq_no(last_sent_pp_key)
            return True
        else:
            return False

    def _can_restore_last_sent_pp_seq_no(self, last_sent_pp_key: PrePrepareKey) -> bool:
        if last_sent_pp_key.view_no != self.node.viewNo:
            logger.info("{} ignoring stored {} because current view no is {}"
                        .format(self.node, last_sent_pp_key, self.node.viewNo))
            return False

        if last_sent_pp_key.inst_id not in self.node.replicas.keys():
            logger.info("{} ignoring stored {} because it does not have replica for instance {}"
                        .format(self.node, last_sent_pp_key, last_sent_pp_key.inst_id))
            return False

        replica = self.node.replicas[last_sent_pp_key.inst_id]

        if replica.isPrimary is not True:
            logger.info("{} ignoring stored {} because it is not primary in instance {}"
                        .format(self.node, last_sent_pp_key, last_sent_pp_key.inst_id))
            return False

        if replica.isMaster:
            logger.warning("{} ignoring stored {} because master's primary "
                           "restores lastPrePrepareSeqNo using catch-up"
                           .format(self.node, last_sent_pp_key))
            return False

        return True

    def _restore_last_sent_pp_seq_no(self, last_sent_pp_key: PrePrepareKey):
        logger.info("{} restoring lastPrePrepareSeqNo from {}"
                    .format(self.node, last_sent_pp_key))
        replica = self.node.replicas[last_sent_pp_key.inst_id]
        replica.lastPrePrepareSeqNo = last_sent_pp_key.pp_seq_no
        replica.last_ordered_3pc = (last_sent_pp_key.view_no, last_sent_pp_key.pp_seq_no)
        replica.update_watermark_from_3pc()

    def _save_last_sent_pp_key(self, pp_key: PrePrepareKey):
        value_as_dict = pp_key._asdict()
        serialized_value = node_status_db_serializer.serialize(value_as_dict)
        self.node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE, serialized_value)

    def _load_last_sent_pp_key(self) -> Optional[PrePrepareKey]:
        if LAST_SENT_PRE_PREPARE not in self.node.nodeStatusDB:
            logger.info("{} did not find stored lastSentPrePrepare"
                        .format(self.node))
            return None

        serialized_value = self.node.nodeStatusDB.get(LAST_SENT_PRE_PREPARE)
        logger.info("{} found stored lastSentPrePrepare value {}"
                    .format(self.node, serialized_value))
        value_as_dict = node_status_db_serializer.deserialize(serialized_value)
        pp_key = PrePrepareKey(**value_as_dict)

        if not isinstance(pp_key.inst_id, int):
            raise TypeError("inst_id must be of int type")
        if not isinstance(pp_key.view_no, int):
            raise TypeError("view_no must be of int type")
        if not isinstance(pp_key.pp_seq_no, int):
            raise TypeError("pp_seq_no must be of int type")

        return pp_key
