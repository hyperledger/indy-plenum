from plenum.common.log import getlogger

logger = getlogger()


def fault(ex: Exception, msg: str):
    logger.error(msg, exc_info=ex)
