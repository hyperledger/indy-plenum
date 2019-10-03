from plenum.test.spy_helpers import get_count


def sum_of_request_propagates(node):
    return get_count(node.replicas[0]._ordering_service,
                     node.replicas[0]._ordering_service._request_propagates_if_needed) + \
           get_count(node.replicas[1]._ordering_service,
                     node.replicas[1]._ordering_service._request_propagates_if_needed)
