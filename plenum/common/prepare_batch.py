from stp_core.common.log import getlogger

SPLIT_STEPS_LIMIT = 8

logger = getlogger()


def split_messages_on_batches(msgs, make_batch_func, is_batch_len_under_limit,
                              step_num=0):

    def split(rec_depth):
        len_2 = len(msgs) // 2
        left_batch = split_messages_on_batches(msgs[:len_2], make_batch_func,
                                               is_batch_len_under_limit, rec_depth)
        right_batch = split_messages_on_batches(msgs[len_2:], make_batch_func,
                                                is_batch_len_under_limit, rec_depth)
        return left_batch + right_batch if left_batch and right_batch else None

    if step_num > SPLIT_STEPS_LIMIT:
        logger.warning('Too many split steps '
                       'were done {}. Batches were not created'.format(step_num))
        return None

    # precondition for case when total length is greater than limit
    # helps skip extra serialization step
    total_len = sum(len(m) for m in msgs)
    if not is_batch_len_under_limit(total_len):
        for m in msgs:
            if not is_batch_len_under_limit(len(m)):
                logger.warning('The message {} is too long ({}). '
                               'Batches were not created'.format(m, len(m)))
                return
        step_num += 1
        return split(step_num)

    # make a batch and check its length
    batch = make_batch_func(msgs)
    if is_batch_len_under_limit(len(batch)):
        return [batch]  # success split
    else:
        if len(msgs) == 1:
            # a batch with this message greater than limit so split fails
            logger.warning('The message {} is less than limit '
                           'but the batch which contains only this '
                           'message has size {} which is greater than '
                           'limit'.format(msgs, len(batch)))
            return None
        step_num += 1
        return split(step_num)
