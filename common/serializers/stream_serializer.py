from abc import ABCMeta, abstractmethod


class StreamSerializer(metaclass=ABCMeta):

    @abstractmethod
    def get_lines(self, stream):
        pass
