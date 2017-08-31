

def split_msgs_on_batches(msgs, make_batch_func, is_batch_len_under_limit):
    # TODO limit recursion depth
    tt_len = sum(len(m) for m in msgs)
    if not is_batch_len_under_limit(tt_len):
        # TODO if we here with one message we have to fail splitting
        # TODO check if each message less than limit
        l = len(msgs) // 2
        # TODO handle border conditions
        left_batch = split_msgs_on_batches(msgs[:l], make_batch_func, is_batch_len_under_limit)
        right_batch = split_msgs_on_batches(msgs[l:], make_batch_func, is_batch_len_under_limit)
        return left_batch + right_batch
    batch = make_batch_func(msgs)
    if is_batch_len_under_limit(len(batch)):
        return [batch]
    else:
        # TODO if we here with one message we have to fail splitting
        l = len(msgs) // 2
        # TODO handle border conditions
        left_batch = split_msgs_on_batches(msgs[:l], make_batch_func, is_batch_len_under_limit)
        right_batch = split_msgs_on_batches(msgs[l:], make_batch_func, is_batch_len_under_limit)
        return left_batch + right_batch
