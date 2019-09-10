from typing import Optional, Dict

from common.serializers.serialization import node_status_db_serializer
from plenum.common.constants import LAST_SENT_PRE_PREPARE
from stp_core.common.log import getlogger

logger = getlogger()


class LastSentPpStoreHelper:

    def __init__(self, node):
        self.node = node

    def store_last_sent_pp_seq_no(self, inst_id: int, pp_seq_no: int):
        stored = self._try_load_last_stored()
        if stored is False:
            return
        new_store = stored or {}
        new_store[str(inst_id)] = (self.node.viewNo, pp_seq_no)
        self._save_last_stored(new_store)

    def erase_last_sent_pp_seq_no(self):
        logger.info("{} erasing stored lastSentPrePrepare".format(self.node))
        if LAST_SENT_PRE_PREPARE in self.node.nodeStatusDB:
            self.node.nodeStatusDB.remove(LAST_SENT_PRE_PREPARE)

    def try_restore_last_sent_pp_seq_no(self):
        logger.info("{} trying to restore lastPrePrepareSeqNo".format(self.node))

        stored = self._try_load_last_stored()
        if stored is False:
            return stored

        if stored is None:
            return False

        someone_restored = False
        for inst_id, pair_3pc in stored.items():
            if self._can_restore_last_sent_pp_seq_no(int(inst_id), pair_3pc):
                self._restore_last_stored(int(inst_id), pair_3pc)
                someone_restored = True
        return someone_restored

    def _can_restore_last_sent_pp_seq_no(self, inst_id, pair_3pc) -> bool:
        stored = (inst_id, pair_3pc)

        if inst_id not in self.node.replicas.keys():
            logger.info("{} ignoring stored {} because it does not have replica for instance {}"
                        .format(self.node, stored, inst_id))
            return False

        replica = self.node.replicas[inst_id]

        if replica.isPrimary is None:
            logger.info("{} ignoring stored {} because it is not primary in instance {}"
                        .format(self.node, stored, inst_id))
            return False

        if replica.isMaster:
            logger.warning("{} ignoring stored {} because master's primary "
                           "restores lastPrePrepareSeqNo using catch-up"
                           .format(self.node, stored))
            return False

        return True

    def _restore_last_stored(self, inst_id, pair_3pc):
        stored = (inst_id, pair_3pc)
        logger.info("{} restoring lastPrePrepareSeqNo from {}"
                    .format(self.node, stored))
        replica = self.node.replicas[inst_id]
        replica._ordering_service.lastPrePrepareSeqNo = pair_3pc[1]
        replica.last_ordered_3pc = (pair_3pc[0], pair_3pc[1])
        # TODO: add the method update_watermark_from_3pc to replica
        # or solve this problem better
        replica._checkpointer.update_watermark_from_3pc()

    def _save_last_stored(self, value: Dict):
        serialized_value = node_status_db_serializer.serialize(value)
        self.node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE, serialized_value)

    def _load_last_sent_pp_key(self) -> Optional[Dict]:
        if LAST_SENT_PRE_PREPARE not in self.node.nodeStatusDB:
            logger.info("{} did not find stored lastSentPrePrepare"
                        .format(self.node))
            return None

        serialized_value = self.node.nodeStatusDB.get(LAST_SENT_PRE_PREPARE)
        logger.debug("{} found stored lastSentPrePrepare value {}"
                     .format(self.node, serialized_value))
        stored = node_status_db_serializer.deserialize(serialized_value)

        if not stored or not isinstance(stored, dict):
            raise TypeError("stored pp_store has wrong format")
        for inst_id, pair_3pc in stored.items():
            if not inst_id.isdigit():
                raise TypeError("inst_id must be of int type")
            if len(pair_3pc) != 2:
                raise TypeError("extra data found")
            if not isinstance(pair_3pc[0], int):
                raise TypeError("view_no must be of int type")
            if not isinstance(pair_3pc[1], int):
                raise TypeError("pp_seq_no must be of int type")

        return stored

    def _try_load_last_stored(self):
        try:
            stored = self._load_last_sent_pp_key()
        except Exception as e:
            logger.warning("{} cannot unpack inst_id, view_no, pp_seq_no "
                           "from stored lastSentPrePrepare: {}"
                           .format(self.node, e))
            return False
        return stored

    def close(self):
        # Need for database_manager
        pass
