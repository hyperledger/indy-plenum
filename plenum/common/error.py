from plenum.common.util import getlogger

logger = getlogger()


def fault(ex: Exception, msg: str):
    logger.error(msg, exc_info=ex)
