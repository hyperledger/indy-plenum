import os

class NodeConfigHelper():
    def __init__(self, name: str, config):
        assert name is not None
        assert config is not None
        self.name = name
        self.config = config

    @property
    def ledger_dir(self):
        config = self.config
        return os.path.join(config.LEDGER_DIR, config.CURRENT_NETWORK, 'data', self.name)

    @property
    def log_dir(self):
        config = self.config
        return os.path.join(config.LOG_DIR, config.CURRENT_NETWORK)

    @property
    def keys_dir(self):
        config = self.config
        return os.path.join(config.KEYS_DIR, config.CURRENT_NETWORK, 'keys', self.name)

    @property
    def genesis_dir(self):
        config = self.config
        return os.path.join(config.GENESIS_DIR, config.CURRENT_NETWORK)

    @property
    def plugins_dir(self):
        return self.config.PLUGINS_DIR


class ClientConfigHelper(NodeConfigHelper):
    @property
    def log_dir(self):
        return self.config.LOG_DIR

    @property
    def wallet_dir(self):
        config = self.config
        return os.path.join(config.WALLET_DIR, config.CURRENT_NETWORK)
