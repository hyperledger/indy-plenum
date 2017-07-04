import pytest

from plenum.common.exceptions import MissingNodeOp, InvalidNodeOp
from plenum.common.messages.fields import NonNegativeNumberField
from plenum.common.messages.message_base import MessageBase
from plenum.common.messages.node_message_factory import MessageFactory, NodeMessageFactory
from plenum.test.input_validation.stub_messages import AMessage1


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
    msg = {'op': 'AMessage1', 'a': 0, 'b': 'bar'}
    assert isinstance(factory.get_instance(**msg), AMessage1)


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
