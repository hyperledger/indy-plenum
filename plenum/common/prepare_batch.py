SPLIT_STEPS_LIMIT = 8


def split_messages_on_batches(msgs, make_batch_func, is_batch_len_under_limit, step_num=0):

    def split(rec_depth):
        l = len(msgs) // 2
        left_batch = split_messages_on_batches(msgs[:l], make_batch_func, is_batch_len_under_limit, rec_depth)
        right_batch = split_messages_on_batches(msgs[l:], make_batch_func, is_batch_len_under_limit, rec_depth)
        return left_batch + right_batch if left_batch and right_batch else None

    if step_num > SPLIT_STEPS_LIMIT:
        return None

    # precondition for case when total length is greater than limit
    # helps skip extra serialization step
    tt_len = sum(len(m) for m in msgs)
    if not is_batch_len_under_limit(tt_len):
        if any(not is_batch_len_under_limit(len(m)) for m in msgs):
            # there is at least one message greater than limit so split fails
            return None
        step_num += 1
        return split(step_num)

    # make a batch and check its length
    batch = make_batch_func(msgs)
    if is_batch_len_under_limit(len(batch)):
        return [batch]  # success split
    else:
        if len(msgs) == 1:
            # a batch with this message greater than limit so split fails
            return None
        step_num += 1
        return split(step_num)
