from storage.binary_file_store import BinaryFileStore
from common.serializers.stream_serializer import StreamSerializer


class BinarySerializerBasedFileStore(BinaryFileStore):
    def __init__(self,
                 serializer: StreamSerializer,
                 dbDir,
                 dbName,
                 isLineNoKey: bool = False,
                 storeContentHash: bool = True,
                 ensureDurability: bool = True,
                 open=True):
        self.serializer = serializer
        super().__init__(dbDir,
                         dbName,
                         isLineNoKey=isLineNoKey,
                         storeContentHash=storeContentHash,
                         ensureDurability=ensureDurability,
                         open=open)
        self.lineSep = b''
        self.delimiter = b''

    def _lines(self):
        # Move to the beginning of file
        self.db_file.seek(0)
        return self.serializer.get_lines(self.db_file)

    def _append_new_line_if_req(self):
        pass
