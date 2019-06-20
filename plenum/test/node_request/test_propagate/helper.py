from plenum.test.spy_helpers import get_count


def sum_of_request_propagates(node):
    return get_count(node.replicas[0], node.replicas[0].request_propagates_if_needed) + \
           get_count(node.replicas[1], node.replicas[1].request_propagates_if_needed)
