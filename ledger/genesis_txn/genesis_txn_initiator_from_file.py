import os

from common.serializers.json_serializer import JsonSerializer
from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.genesis_txn.genesis_txn_file_util import genesis_txn_file
from ledger.genesis_txn.genesis_txn_initiator import GenesisTxnInitiator
from ledger.ledger import Ledger
from storage import store_utils
from storage.text_file_store import TextFileStore
from stp_core.common.log import getlogger

logger = getlogger()


class GenesisTxnInitiatorFromFile(GenesisTxnInitiator):
    """
    Creates genesis txn as a text file.
    Can init the ledger from a text file-based genesis txn file.
    """

    def __init__(self, data_dir, txn_file, serializer=JsonSerializer()):
        self.__data_dir = data_dir
        self.__db_name = genesis_txn_file(txn_file)
        self.__serializer = serializer

    def init_ledger_from_genesis_txn(self, ledger: Ledger):
        # TODO: it's possible that the file to be used for initialization does not exist.
        # This is not considered as an error as of now.
        init_file = os.path.join(self.__data_dir, self.__db_name)
        if not os.path.exists(init_file):
            logger.display("File that should be used for initialization of "
                           "Ledger does not exist: {}".format(init_file))
            return

        with open(init_file, 'r') as f:
            for line in store_utils.cleanLines(f):
                txn = self.__serializer.deserialize(line)
                ledger.add(txn)

    def create_initiator_ledger(self) -> Ledger:
        store = TextFileStore(self.__data_dir,
                              self.__db_name,
                              isLineNoKey=True,
                              storeContentHash=False,
                              ensureDurability=False)
        return Ledger(CompactMerkleTree(),
                      dataDir=self.__data_dir,
                      txn_serializer=self.__serializer,
                      fileName=self.__db_name,
                      transactionLogStore=store)
