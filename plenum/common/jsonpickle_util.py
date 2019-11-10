from enum import Enum

from jsonpickle import tags
from jsonpickle.unpickler import loadclass
from jsonpickle.handlers import register, BaseHandler

ENUMVALUE = 'py/enumvalue'


class EnumHandler(BaseHandler):
    """
    Jsonpickle handler for enumerations.
    Used to avoid the issue https://github.com/jsonpickle/jsonpickle/issues/135
    in jsonpickle 0.9.2 which is the canonical version for Ubuntu 16.04.
    Provides a custom format for serialization of enumerations.
    """

    def flatten(self, obj, data):
        data[ENUMVALUE] = obj.value
        return data

    def restore(self, obj):
        enum_class = loadclass(obj[tags.OBJECT])
        return enum_class(obj[ENUMVALUE])


def setUpJsonpickle():
    register(Enum, EnumHandler, base=True)
