import logging
import os

from common.serializers.json_serializer import JsonSerializer
from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.genesis_txn.genesis_txn_initiator import GenesisTxnInitiator
from ledger.ledger import Ledger
from storage import store_utils
from storage.text_file_store import TextFileStore


class GenesisTxnInitiatorFromFile(GenesisTxnInitiator):

    def __init__(self, data_dir, db_name, serializer=JsonSerializer()):
        self.__data_dir = data_dir
        self.__db_name = db_name
        self.__init_file = os.path.join(self.__data_dir, self.__db_name)
        self.__serializer = serializer

    def init_ledger_from_genesis_txn(self, ledger: Ledger):
        if not self.__init_file:
            return
        if not os.path.exists(self.__init_file):
            errMessage = "File that should be used for " \
                         "initialization of Ledger does not exist: {}"\
                         .format(self.__init_file)
            logging.warning(errMessage)
            raise ValueError(errMessage)

        with open(self.__init_file, 'r') as f:
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
