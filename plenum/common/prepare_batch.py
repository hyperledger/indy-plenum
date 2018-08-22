from stp_core.common.log import getlogger

SPLIT_STEPS_LIMIT = 8

logger = getlogger()


def split_messages_on_batches(msgs, make_batch_func, is_batch_len_under_limit):

    batches = []
    while msgs:
        batch = ''
        msgs_for_batch = []
        while msgs and is_batch_len_under_limit(len(batch)):
            msg = msgs.pop(0)
            serialized_msg = make_batch_func([msg])
            if not is_batch_len_under_limit(len(serialized_msg)):
                logger.display('The message {}... is too long ({}). '
                               'Batches were not created'.format(serialized_msg[:256], len(serialized_msg)))
                return
            msgs_for_batch.append(msg)
            batch = make_batch_func(msgs_for_batch)
        """Need for limitation one batch size"""
        if not is_batch_len_under_limit(len(batch)):
            overload_msg = msgs_for_batch.pop()
            batch = make_batch_func(msgs_for_batch)
            msgs.insert(0, overload_msg)
        batches.append((batch, len(msgs_for_batch)))
    if len(batches) == 0:
        batches = [(make_batch_func([]), 0)]
    return batches
