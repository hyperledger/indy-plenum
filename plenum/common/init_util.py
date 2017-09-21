from plenum.common.has_file_storage import HasFileStorage
from plenum.common.keygen_utils import initNodeKeysForBothStacks


def cleanup_environment(name, base_dir):
    dataLocation = HasFileStorage.getDataLocation(name, base_dir)
    HasFileStorage.wipeDataLocation(dataLocation)


def initialize_node_environment(name, base_dir, sigseed=None,
                                override_keep=False):
    cleanup_environment(name, base_dir)

    _, vk, bls_key = initNodeKeysForBothStacks(name=name, baseDir=base_dir,
                                               sigseed=sigseed, override=override_keep)
    return vk, bls_key
