import pytest
from plenum.common.messages.node_messages import MessageFactory, NodeMessageFactory
from plenum.test.input_validation.stub_messages import AMessage1


def test_message_factory_module_is_not_found_fails():
    with pytest.raises(AssertionError):
        MessageFactory('foo.bar')


def test_message_factory_stub_module_is_loaded():
    f = MessageFactory('plenum.test.input_validation.stub_messages')
    assert f({'op': 'AMessage1', 'a': 0, 'b': 'bar'}) == AMessage1


def test_node_message_factory_module_is_loaded():
    NodeMessageFactory()
