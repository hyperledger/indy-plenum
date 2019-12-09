from indy_crypto.bls import VerKey, SignKey

from crypto.bls.bls_crypto import BlsGroupParamsLoader, GroupParams
from crypto.bls.bls_factory import BlsFactoryCrypto
from crypto.bls.bls_key_manager import BlsKeyManager
from crypto.bls.indy_crypto.bls_crypto_indy_crypto import BlsGroupParamsLoaderIndyCrypto, BlsCryptoSignerIndyCrypto, \
    BlsCryptoVerifierIndyCrypto, IndyCryptoBlsUtils
from plenum.bls.bls_key_manager_file import BlsKeyManagerFile


class BlsFactoryIndyCrypto(BlsFactoryCrypto):
    def __init__(self, keys_dir=None):
        self._keys_dir = keys_dir

    def generate_bls_keys(self, seed=None) -> (str, str, str):
        sk, pk, key_proof = self._generate_raw_bls_keys(seed)
        return IndyCryptoBlsUtils.bls_to_str(sk), IndyCryptoBlsUtils.bls_to_str(pk), IndyCryptoBlsUtils.bls_to_str(
            key_proof)

    def _create_group_params_loader(self) -> BlsGroupParamsLoader:
        return BlsGroupParamsLoaderIndyCrypto()

    def _get_bls_crypto_signer_class(self):
        return BlsCryptoSignerIndyCrypto

    def _create_bls_crypto_signer(self, sk: str, pk: str, group_params: GroupParams):
        sk_bls = IndyCryptoBlsUtils.bls_from_str(sk, cls=SignKey)  # type: SignKey
        pk_bls = IndyCryptoBlsUtils.bls_from_str(pk, cls=VerKey)  # type: VerKey
        return BlsCryptoSignerIndyCrypto(sk=sk_bls, pk=pk_bls, params=group_params)

    def _create_bls_crypto_verifier(self, group_params):
        return BlsCryptoVerifierIndyCrypto(group_params)

    def _create_key_manager(self, group_params) -> BlsKeyManager:
        assert self._keys_dir
        return BlsKeyManagerFile(self._keys_dir)


def create_default_bls_crypto_factory(keys_dir=None):
    '''
    Creates a default BLS factory to instantiate BLS crypto classes.

    :param keys_dir: [optional] keys_dir; needed to save/load bls keys
    :return: BLS factory instance
    '''
    return BlsFactoryIndyCrypto(keys_dir)
