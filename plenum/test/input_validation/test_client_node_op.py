import pytest
from plenum.common.constants import BLS_KEY

from plenum.common.messages.client_request import ClientNodeOperationData, ALIAS, SERVICES, NODE_IP, CLIENT_IP, \
    NODE_PORT, CLIENT_PORT

op = ClientNodeOperationData()


def test_only_alias_passes():
    op.validate({ALIAS: 'aNode'})


def test_empty_alias_fails():
    with pytest.raises(TypeError) as ex_info:
        op.validate({ALIAS: ''})
    ex_info.match(
        'validation error \[ClientNodeOperationData\]: empty string \(alias=\)')


def test_missed_alias_fails():
    with pytest.raises(Exception) as ex_info:
        op.validate({SERVICES: []})
    ex_info.match(
        'validation error \[ClientNodeOperationData\]: missed fields - alias')


def test_missed_a_ha_field_fails():
    with pytest.raises(Exception) as ex_info:
        op.validate({
            ALIAS: 'aNode',
            NODE_PORT: 9700,
            CLIENT_IP: '8.8.8.8',
            CLIENT_PORT: 9701,
        })
    ex_info.match(
        'validation error \[ClientNodeOperationData\]: missed fields - node_ip')


def test_update_services_passes():
    op.validate({ALIAS: 'aNode', SERVICES: []})


def test_update_ha_passes():
    op.validate({
        ALIAS: 'aNode',
        NODE_IP: '8.8.8.8',
        NODE_PORT: 9700,
        CLIENT_IP: '8.8.8.8',
        CLIENT_PORT: 9701,
    })


def test_update_bls_sign():
    op.validate({
        ALIAS: 'aNode',
        BLS_KEY: 'some_key',
    })


def test_empty_bls_fails():
    with pytest.raises(TypeError) as ex_info:
        op.validate({
            BLS_KEY: '',
            ALIAS: 'aNode'
        })
    ex_info.match(
        'validation error \[ClientNodeOperationData\]: empty string \(blskey=\)')
