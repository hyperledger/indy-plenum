import os

from crypto.bls.bls_key_manager import BlsKeyManager


class BlsKeyManagerFile(BlsKeyManager):
    BLS_KEYS_DIR_NAME = 'bls_keys'
    BLS_SK_FILE_NAME = 'bls_sk'
    BLS_PK_FILE_NAME = 'bls_pk'

    BLS_KEYS_DIR_MODE = 0o744
    BLS_SK_FILE_MODE = 0o600
    BLS_PK_FILE_MODE = 0o644

    def __init__(self, keys_dir):
        self._keys_dir = keys_dir
        self._init_dirs()

    def _init_dirs(self):
        os.makedirs(
            self._keys_dir,
            exist_ok=True)
        self._bls_keys_dir = os.path.join(self._keys_dir, self.BLS_KEYS_DIR_NAME)
        os.makedirs(
            self._bls_keys_dir,
            self.BLS_KEYS_DIR_MODE,
            exist_ok=True)

    def _save_secret_key(self, sk: str):
        self.__save_to_file(sk, self.BLS_SK_FILE_NAME, self.BLS_SK_FILE_MODE)

    def _save_public_key(self, pk: str):
        self.__save_to_file(pk, self.BLS_PK_FILE_NAME, self.BLS_PK_FILE_MODE)

    def _load_secret_key(self) -> str:
        return self.__load_from_file(self.BLS_SK_FILE_NAME)

    def _load_public_key(self) -> str:
        return self.__load_from_file(self.BLS_PK_FILE_NAME)

    def __save_to_file(self, key: str, name, mode):
        path = os.path.join(self._bls_keys_dir, name)
        with open(path, 'wb') as f:
            f.write(key.encode())
        os.chmod(path, mode)

    def __load_from_file(self, name) -> str:
        path = os.path.join(self._bls_keys_dir, name)
        with open(path, 'rb') as f:
            key = f.read()
        return key.decode('utf-8')
