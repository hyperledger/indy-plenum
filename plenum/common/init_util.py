from plenum.common.has_file_storage import HasFileStorage
from plenum.common.keygen_utils import initLocalKeys, initNodeKeysForBothStacks


def cleanup_environment(name, base_dir):
    dataLocation = HasFileStorage.getDataLocation(name, base_dir)
    HasFileStorage.wipeDataLocation(dataLocation)


def initialize_node_environment(name, base_dir, sigseed=None,
                                override_keep=False):
    cleanup_environment(name, base_dir)

    _, vk = initNodeKeysForBothStacks(name=name, baseDir=base_dir,
                                      sigseed=sigseed, override=override_keep)

    return vk
