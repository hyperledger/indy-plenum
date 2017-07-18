from ledger.serializers.json_serializer import JsonSerializer


def testJsonSerializer():
    sz = JsonSerializer()
    m1 = {'integer': 36, 'name': 'Foo', 'surname': 'Bar', 'float': 14.8639,
          'index': 1, 'index_start_at': 56, 'email': 'foo@bar.com',
          'fullname': 'Foo Bar', 'bool': False}
    m1s = '{"bool":false,"email":"foo@bar.com","float":14.8639,"fullname":"Foo Bar","index":1,"index_start_at":56,"integer":36,"name":"Foo","surname":"Bar"}'

    m2 = {'latitude': 31.351883, 'longitude': -97.466179, 
          'tags': ['foo', 'bar', 'baz', 'alice', 'bob',
                   'carol', 'dave']}
    m2s = '{"latitude":31.351883,"longitude":-97.466179,"tags":["foo","bar","baz","alice","bob","carol","dave"]}'

    m3 = {'name': 'Alice Bob', 'website': 'example.com', 'friends': [
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
      }]}
    m3s = '{"friends":[{"id":0,"name":"Dave"},{"id":1,"name":"Carol"},{"id":2,"name":"Dave"}],"name":"Alice Bob","website":"example.com"}'

    assert sz.serialize(m1) == m1s.encode()
    assert sz.serialize(m1, toBytes=False) == m1s
    assert sz.serialize(m2) == m2s.encode()
    assert sz.serialize(m2, toBytes=False) == m2s
    assert sz.serialize(m3) == m3s.encode()
    assert sz.serialize(m3, toBytes=False) == m3s

    assert sz.deserialize(m1s) == m1
    assert sz.deserialize(m1s.encode()) == m1
    assert sz.deserialize(m2s) == m2
    assert sz.deserialize(m2s.encode()) == m2
    assert sz.deserialize(m3s) == m3
    assert sz.deserialize(m3s.encode()) == m3
