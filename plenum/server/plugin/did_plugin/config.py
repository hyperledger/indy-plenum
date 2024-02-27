from plenum.common.constants import KeyValueStorageType


def get_config(config):
    config.didPluginTransactionsFile = 'did_plugin_transactions'
    config.didPluginStateStorage = KeyValueStorageType.Leveldb
    config.didPluginStateDbName = 'did_plugin_state'
    return config
