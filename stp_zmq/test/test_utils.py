from stp_zmq.test.helper import get_file_permission_mask
from stp_zmq.util import createCertsFromKeys


def test_create_certs_from_fromkeys_sets_600_for_secret_644_for_pub_keys(tdir):
    public_key_file, secret_key_file = createCertsFromKeys(
        tdir, 'akey', b'0123456789')
    assert get_file_permission_mask(secret_key_file) == '600'
    assert get_file_permission_mask(public_key_file) == '644'
