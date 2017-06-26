import sys

from plenum.common.constants import OP_FIELD_NAME
from plenum.common.messages.message_base import MessageBase


class MessageFactory:

    def __init__(self, class_module_name):
        self.__classes = self.__get_message_classes(class_module_name)
        assert len(self.__classes) > 0, \
            "at least one message class loaded"

    def __call__(self, message_raw):
        message_op = message_raw.pop(OP_FIELD_NAME)
        message_cls = self.__classes.get(message_op, None)
        # TODO better to create an instance here
        return message_cls

    @classmethod
    def __get_message_classes(cls, class_module_name):
        assert class_module_name in sys.modules, \
            "{} is loaded".format(class_module_name)
        class_module = sys.modules[class_module_name]
        classes = {}
        for x in dir(class_module):
            obj = getattr(class_module, x)
            if cls.__is_node_message(obj):
                classes.update({obj.typename: obj})
        return classes

    @staticmethod
    def __is_node_message(obj):
        return getattr(obj, "schema", None) and issubclass(obj, MessageBase)


class NodeMessageFactory(MessageFactory):

    def __init__(self):
        super().__init__('plenum.common.types')
