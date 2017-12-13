from abc import abstractmethod


class HasCliCommands:
    @property
    def cli(self):
        return self._cli

    @cli.setter
    def cli(self, cli):
        self._cli = cli

    @property
    @abstractmethod
    def actions(self):
        pass
