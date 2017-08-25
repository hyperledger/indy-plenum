import logging
import os

from storage import store_utils
from storage.kv_store_single_file import SingleFileStore


class TextFileStore(SingleFileStore):

    def __init__(self,
                 dbDir,
                 dbName,
                 isLineNoKey: bool=False,
                 storeContentHash: bool=True,
                 ensureDurability: bool=True,
                 open=True):
        super().__init__(dbDir,
                         dbName,
                         delimiter='\t',
                         lineSep='\r\n',
                         isLineNoKey=isLineNoKey,
                         storeContentHash=storeContentHash,
                         ensureDurability=ensureDurability,
                         open=open)

    @property
    def is_byte(self) -> bool:
        return False

    def _init_db_path(self, dbDir, dbName):
        return os.path.join(os.path.expanduser(dbDir), dbName)

    def _init_db_file(self):
        return open(self.db_path, mode="a+")

    def _lines(self):
        # Move to the beginning of file
        self.db_file.seek(0)
        return store_utils.cleanLines(self.db_file)

    def _append_new_line_if_req(self):
        try:
            logging.debug("new line check for file: {}".format(self.db_path))
            with open(self.db_path, 'a+b') as f:
                size = f.tell()
                if size > 0:
                    f.seek(-len(self.lineSep), 2)  # last character in file
                    if f.read().decode() != self.lineSep:
                        linesep = self.lineSep if isinstance(
                            self.lineSep, bytes) else self.lineSep.encode()
                        f.write(linesep)
                        logging.debug(
                            "new line added for file: {}".format(self.db_path))
        except FileNotFoundError:
            pass
