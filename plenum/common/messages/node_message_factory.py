import sys
from importlib import import_module

from plenum.common.constants import OP_FIELD_NAME
from plenum.common.exceptions import MissingNodeOp, InvalidNodeOp
from plenum.common.messages.fields import IterableField, MapField
from plenum.common.messages.message_base import MessageBase


class MessageFactory:

    def __init__(self, class_module_name):
        classes_module = self.__load_module_by_name(class_module_name)
        self.__classes = self.__get_message_classes(classes_module)
        assert len(self.__classes) > 0, "at least one message class loaded"

    @classmethod
    def __load_module_by_name(cls, module_name):
        the_module = cls.__get_module_by_name(module_name)
        if the_module is not None:
            return the_module

        import_module(module_name)  # can raise ImportError
        the_module = cls.__get_module_by_name(module_name)
        return the_module

    @classmethod
    def __get_message_classes(cls, classes_module):
        classes = {}
        for x in dir(classes_module):
            obj = getattr(classes_module, x)
            doesnt_fit_reason = cls.__check_obj_fits(obj)
            if doesnt_fit_reason is None:
                classes.update({obj.typename: obj})
        return classes

    def get_instance(self, **message_raw):
        message_op = message_raw.get(OP_FIELD_NAME, None)
        if message_op is None:
            raise MissingNodeOp
        cls = self.get_type(message_op)
        msg = self.__msg_without_op_field(message_raw)
        return cls(**msg)

    def get_type(self, message_op):
        message_cls = self.__classes.get(message_op, None)
        if message_cls is None:
            raise InvalidNodeOp(message_op)
        return message_cls

    @staticmethod
    def __msg_without_op_field(msg):
        return {k: v for k, v in msg.items() if k != OP_FIELD_NAME}

    def set_message_class(self, message_class):
        doesnt_fit_reason = self.__check_obj_fits(message_class)
        assert not doesnt_fit_reason, doesnt_fit_reason
        self.__classes.update({message_class.typename: message_class})

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

    # TODO: it is a workaround which helps extend some fields from
    # downstream projects, should be removed after we find a better way
    # to do this
    def update_schemas_by_field_type(self, old_field_type, new_field_type):
        for cls in self.__classes.values():
            new_schema = []
            for name, field in cls.schema:
                field = self._transform_field(
                    field, old_field_type, new_field_type)
                new_schema.append((name, field))
            cls.schema = tuple(new_schema)

    def _transform_field(self, field, old_field_type, new_field_type):
        if isinstance(field, old_field_type):
            return new_field_type()
        elif self.__is_iterable_and_contains_type(field, old_field_type):
            return IterableField(new_field_type())
        elif isinstance(field, MapField):
            key = field.key_field
            val = field.value_field
            if isinstance(field.key_field, old_field_type):
                key = new_field_type()
            if isinstance(field.value_field, old_field_type):
                val = new_field_type()
            return MapField(key, val)
        return field

    @staticmethod
    def __is_iterable_and_contains_type(field, field_type):
        return isinstance(field, IterableField) and \
            isinstance(field.inner_field_type, field_type)


class NodeMessageFactory(MessageFactory):

    def __init__(self):
        super().__init__('plenum.common.messages.node_messages')


node_message_factory = NodeMessageFactory()
