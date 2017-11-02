
def test_client_can_send_write_requests(looper, txnPoolNodeSet, client1):
    looper.add(client1)

    def can_send():
        if not client1.can_send_write_requests():
            raise Exception('Can not send write requests')



def test_client_can_send_read_requests():
    pass

def test_client_can_send_request():
    pass