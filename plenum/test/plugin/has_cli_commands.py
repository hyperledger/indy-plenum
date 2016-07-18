from abc import abstractproperty


class HasCliCommands:
    @property
    def cli(self):
        return self._cli

    @cli.setter
    def cli(self, cli):
        self._cli = cli

    @abstractproperty
    def actions(self):
        pass
