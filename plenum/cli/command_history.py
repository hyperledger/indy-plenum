from prompt_toolkit.history import FileHistory


class CliFileHistory(FileHistory):

    def __init__(self, command_filter=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cmd_filter = command_filter or self.__default_cmd_filter

    def __default_cmd_filter(self, command: str):
        return command

    def append(self, string):
        new_str = self.cmd_filter(string)
        super().append(new_str)
