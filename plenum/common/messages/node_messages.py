import sys
from importlib import import_module

from plenum.common.constants import OP_FIELD_NAME
from plenum.common.exceptions import MissingNodeOp, InvalidNodeOp
from plenum.common.messages.message_base import MessageBase


class MessageFactory:

    def __init__(self, class_module_name):
        classes_module = self.__load_module_by_name(class_module_name)
        self.__classes = self.__get_message_classes(classes_module)
        assert len(self.__classes) > 0, "at least one message class loaded"

    def set_message_class(self, message_class):
        doesnt_fit_reason = self.__check_obj_fits(message_class)
        assert not doesnt_fit_reason, doesnt_fit_reason
        self.__classes.update({message_class.typename: message_class})

    def __call__(self, **message_raw):
        message_op = message_raw.pop(OP_FIELD_NAME, None)
        if message_op is None:
            raise MissingNodeOp
        message_cls = self.__classes.get(message_op, None)
        if message_cls is None:
            raise InvalidNodeOp(message_op)
        return message_cls(**message_raw)

    @classmethod
    def __get_message_classes(cls, classes_module):
        classes = {}
        for x in dir(classes_module):
            obj = getattr(classes_module, x)
            doesnt_fit_reason = cls.__check_obj_fits(obj)
            if doesnt_fit_reason is None:
                classes.update({obj.typename: obj})
        return classes

    @classmethod
    def __load_module_by_name(cls, module_name):
        the_module = cls.__get_module_by_name(module_name)
        if the_module is not None:
            return the_module

        import_module(module_name)  # can raise ImportError
        the_module = cls.__get_module_by_name(module_name)
        return the_module

    @staticmethod
    def __get_module_by_name(module_name):
        return sys.modules.get(module_name, None)

    @staticmethod
    def __check_obj_fits(obj):
        if not getattr(obj, "schema", None):
            return "must have a non empty 'schema'"
        if not getattr(obj, "typename", None):
            return "must have a non empty 'typename'"
        # has to be the last because of: 'str' week ref error
        if not issubclass(obj, MessageBase):
            return "must be a subclass of 'MessageBase'"


class NodeMessageFactory(MessageFactory):

    def __init__(self):
        super().__init__('plenum.common.types')


node_message_factory = NodeMessageFactory()
