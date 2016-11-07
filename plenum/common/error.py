from plenum.common.log import getlogger

logger = getlogger()


def fault(ex: Exception, msg: str):
    logger.error(msg, exc_info=ex)


def error(msg: str) -> Exception:
    """
    Wrapper to get around Python's distinction between statements and expressions
    Can be used in lambdas and expressions such as: a if b else error(c)

    :param msg: error message
    """
    raise Exception(msg)
