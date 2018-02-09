from plenum.common.has_file_storage import HasFileStorage
from plenum.common.keygen_utils import initNodeKeysForBothStacks


def cleanup_environment(dataLocation):
    dataLocation = HasFileStorage.getDataLocation(dataLocation)
    HasFileStorage.wipeDataLocation(dataLocation)


def initialize_node_environment(name, node_config_helper, sigseed=None,
                                override_keep=False):
    cleanup_environment(node_config_helper.ledger_dir)

    _, vk, bls_key = initNodeKeysForBothStacks(name=name, keys_dir=node_config_helper.keys_dir,
                                               sigseed=sigseed, override=override_keep)
    return vk, bls_key
