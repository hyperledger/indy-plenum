# TODO: move it to plenum-util repo


def fault(ex: Exception, msg: str):
    from stp_core.common.log import getlogger
    getlogger().error(msg, exc_info=ex)


def error(msg: str) -> Exception:
    """
    Wrapper to get around Python's distinction between statements and expressions
    Can be used in lambdas and expressions such as: a if b else error(c)

    :param msg: error message
    """
    raise Exception(msg)
