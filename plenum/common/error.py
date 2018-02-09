# TODO: remove this


def fault(ex: Exception, msg: str):
    from stp_core.common.log import getlogger
    getlogger().error(msg, exc_info=ex)
