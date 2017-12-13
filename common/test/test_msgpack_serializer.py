from common.serializers.msgpack_serializer import MsgPackSerializer

serializer = MsgPackSerializer()


def check(value):
    assert value == serializer.deserialize(serializer.serialize(value))


def test_serialize_int():
    check(1)
    check(111324324324324)
    check(-1)
    check(-44445423453245)
    check(0)


def test_serialize_str():
    check('aaaa')
    check('abc def')
    check('a b c d e f')
    check('строка')


def test_serialize_list():
    check([1, 'a', 'b'])


def test_serialize_float():
    check(0.34)
    check(-9.005)
    check(1231231.121212123)
    check(-1231231.121212123)


def test_serialize_ordered_dict():
    check({1: 'a', 2: 'b', 3: 'c'})
    check({3: 'a', 1: 'b', 2: 'c'})
    check({'ccc': 22, 'dd': 11, 'a': 44})


def test_serialize_sub_dict():
    check(
        {"data": {"alias": "danube", "client_ip": "128.130.204.35", "client_port": 9722, "node_ip": "128.130.204.35",
                  "node_port": 9721, "services": ["VALIDATOR"]}, "dest": "GNEcgS1ZKFLvXbjgjtNJSzQsoYHsBpuCM5UwZC791TPX",
         "identifier": "mBeb2wq5fA3vtdPHCfYTbi9bTBuUQE4ydba9rkQdegz",
         "txnId": "ebf340b317c044d970fcd0ca018d8903726fa70c8d8854752cd65e29d443686c", "type": "0"})
    check({"data": {"alias": "BIGAWSUSEAST1-001", "client_ip": "34.224.255.108", "client_port": 9796,
                    "node_ip": "34.224.255.108", "node_port": 9769, "services": ["VALIDATOR"]},
           "dest": "JDCd91PAZr9oEBkwwrhMgG5CGs27HRhCbieLN5H7XBF8",
           "identifier": "6M6oh9Giq88iHRaW7jEj6hksfu99x12GPqNQFU5YLwp8",
           "txnId": "40fceb5fea4dbcadbd270be6d5752980e89692151baf77a6bb64c8ade42ac148", "type": "0"})
    check({"data": {"alias": "BYU", "client_ip": "54.71.209.105", "client_port": 9722, "node_ip": "54.71.209.105",
                    "node_port": 9711, "services": ["VALIDATOR"]},
           "dest": "zCxu3oxVGuuKf7rv58wH6hWeUH6gtEjUQ35eUHkXTYt",
           "identifier": "B4paSwxrLVBwaKb7PXMq7EVWeXQvQCwx6HrxUxvuojns",
           "txnId": "ceecfd37686a3ed1759d3cef25e412a800fc8e8846154dbe2a2d72b2af3e3b64", "type": "0"})
    check({"data": {"alias": "ev1", "client_ip": "54.94.255.14", "client_port": 9702, "node_ip": "54.94.255.14",
                    "node_port": 9701, "services": ["VALIDATOR"]},
           "dest": "6DAHgFWMDxpXdpAPU6GcSfw6xPEi9fUsH3Z3tFeyicGW",
           "identifier": "DiRwDxvUuNdFCP8kxEWZ51qFyxS4qPjfCPbgEfukvKYU",
           "txnId": "b0c82a3ade3497964cb8034be915da179459287823d92b5717e6d642784c50e6", "type": "0"})


def test_serialize_complex_dict():
    check(
        {'integer': 36, 'name': 'Foo', 'surname': 'Bar', 'float': 14.8639,
         'index': 1, 'index_start_at': 56, 'email': 'foo@bar.com',
         'fullname': 'Foo Bar', 'bool': False})
    check(
        {'latitude': 31.351883, 'longitude': -97.466179,
         'tags': ['foo', 'bar', 'baz', 'alice', 'bob',
                  'carol', 'dave']})
    check(
        {'name': 'Alice Bob', 'website': 'example.com', 'friends': [
            {
                'id': 0,
                'name': 'Dave'
            },
            {
                'id': 1,
                'name': 'Carol'
            },
            {
                'id': 2,
                'name': 'Dave'
            }]})
