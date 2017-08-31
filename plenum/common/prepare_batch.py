
# try to split on
SPLIT_STEPS_LIMIT = 8


def split_msgs_on_batches(msgs, make_batch_func, is_batch_len_under_limit, rec_depth=0):
    if rec_depth > SPLIT_STEPS_LIMIT:
        return None
    tt_len = sum(len(m) for m in msgs)
    if not is_batch_len_under_limit(tt_len):
        if any(not is_batch_len_under_limit(len(m)) for m in msgs):
            return None
        if len(msgs) == 1:
            return None
        rec_depth += 1
        l = len(msgs) // 2
        left_batch = split_msgs_on_batches(msgs[:l], make_batch_func, is_batch_len_under_limit, rec_depth)
        right_batch = split_msgs_on_batches(msgs[l:], make_batch_func, is_batch_len_under_limit, rec_depth)
        return left_batch + right_batch if left_batch and right_batch else None
    batch = make_batch_func(msgs)
    if is_batch_len_under_limit(len(batch)):
        return [batch]
    else:
        if any(not is_batch_len_under_limit(len(m)) for m in msgs):
            return None
        if len(msgs) == 1:
            return None
        l = len(msgs) // 2
        rec_depth += 1
        left_batch = split_msgs_on_batches(msgs[:l], make_batch_func, is_batch_len_under_limit, rec_depth)
        right_batch = split_msgs_on_batches(msgs[l:], make_batch_func, is_batch_len_under_limit, rec_depth)
        return left_batch + right_batch if left_batch and right_batch else None
