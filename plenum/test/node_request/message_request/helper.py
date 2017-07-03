def count_msg_reqs_of_type(node, typ):
    return sum([1 for entry in node.spylog.getAll(node.process_message_req)
                if entry.params['msg'].msg_type == typ])
