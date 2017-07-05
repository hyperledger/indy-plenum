import pytest

from plenum.common.exceptions import MissingNodeOp, InvalidNodeOp
from plenum.common.messages.fields import NonNegativeNumberField, AnyValueField, HexField, BooleanField, Base58Field
from plenum.common.messages.message_base import MessageBase
from plenum.common.messages.node_message_factory import MessageFactory, NodeMessageFactory
from plenum.test.input_validation.stub_messages import Message1, Message2, Message3, Message4


@pytest.fixture
def factory():
    return MessageFactory('plenum.test.input_validation.stub_messages')


def test_message_factory_module_is_not_found_fails():
    with pytest.raises(ImportError):
        MessageFactory('foo.bar')


def test_message_factory_missed_op_fails(factory):
    msg = {'a': 0, 'b': 'bar'}
    with pytest.raises(MissingNodeOp):
        factory.get_instance(**msg)


def test_message_factory_invalid_op_fails(factory):
    msg = {'op': 'unknown_op', 'a': 0, 'b': 'bar'}
    with pytest.raises(InvalidNodeOp):
        factory.get_instance(**msg)


def test_message_factory_stub_module_is_loaded(factory):
    msg = {'op': 'Message1', 'a': 0, 'b': 'bar'}
    assert isinstance(factory.get_instance(**msg), Message1)


def test_message_factory_set_non_message_class_fails(factory):
    class NonMessageClass:
        pass

    with pytest.raises(AssertionError):
        factory.set_message_class(NonMessageClass)


def test_message_factory_set_message_class_can_add_message_class(factory):
    class ANewMessageClass(MessageBase):
        typename = 'NewMessage'
        schema = (
            ('a', NonNegativeNumberField()),
        )

    factory.set_message_class(ANewMessageClass)
    msg = {'op': 'NewMessage', 'a': 0}
    assert isinstance(factory.get_instance(**msg), ANewMessageClass)


def test_node_message_factory_module_is_loaded():
    NodeMessageFactory()


def test_message_factory_can_replace_field(factory):
    # check precondition
    msg = {'op': 'Message2', 'a': 0, 'b': 'foo'}
    assert isinstance(factory.get_instance(**msg), Message2)

    factory.update_schemas_by_field_type(AnyValueField, NonNegativeNumberField)

    with pytest.raises(TypeError) as exc_info:
        factory.get_instance(**msg)
    exc_info.match("expected types 'int', got 'str'")


def test_message_factory_can_replace_iterable_field(factory):
    # check precondition
    msg = {'op': 'Message3', 'a': 0, 'b': [True, False]}
    assert isinstance(factory.get_instance(**msg), Message3)

    factory.update_schemas_by_field_type(BooleanField, Base58Field)

    with pytest.raises(TypeError) as exc_info:
        factory.get_instance(**msg)
    exc_info.match("expected types 'str', got 'bool'")


def test_message_factory_can_replace_map_field(factory):
    # check precondition
    msg = {'op': 'Message4', 'a': 0, 'b': {'123': 'abc'}}
    assert isinstance(factory.get_instance(**msg), Message4)

    factory.update_schemas_by_field_type(HexField, NonNegativeNumberField)

    with pytest.raises(TypeError) as exc_info:
        factory.get_instance(**msg)
    exc_info.match("expected types 'int', got 'str'")
