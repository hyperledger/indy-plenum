import time

from common.exceptions import PlenumTypeError, PlenumValueError
from stp_core.common.log import getlogger

logger = getlogger()


class Throttler:

    def __init__(self, windowSize, delayFunction=None):
        """
        Limits rate of actions performed in a unit of time (window)

        :param windowSize: size (in seconds) of the time window events counted in
        :param delayFunction: function from **number of actions** to **time to wait after the last one**
        """
        if not isinstance(windowSize, int):
            raise PlenumTypeError('windowSize', windowSize, int)
        if not windowSize > 0:
            raise PlenumValueError('windowSize', windowSize, '> 0')

        self.windowSize = windowSize
        self.delayFunction = delayFunction if delayFunction else self._defaultDelayFunction
        self.actionsLog = []

    def acquire(self):
        """
        Acquires lock for action.

        :return: True and 0.0 if lock successfully acquired or False and number of seconds to wait before the next try
        """
        now = time.perf_counter()
        logger.debug("now: {}, len(actionsLog): {}".format(
            now, len(self.actionsLog)))
        self._trimActionsLog(now)
        logger.debug("after trim, len(actionsLog): {}".format(
            len(self.actionsLog)))

        if len(self.actionsLog) == 0:
            self.actionsLog.append(now)
            logger.debug("len(actionsLog) was 0, after append, len(actionsLog):"
                         " {}".format(len(self.actionsLog)))
            return True, 0.0
        timeToWaitAfterPreviousTry = self.delayFunction(len(self.actionsLog))
        timePassed = now - self.actionsLog[-1]
        logger.debug("timeToWaitAfterPreviousTry: {}, timePassed: {}".
                     format(timeToWaitAfterPreviousTry, timePassed))
        if timeToWaitAfterPreviousTry < timePassed:
            self.actionsLog.append(now)
            logger.debug(
                "timeToWaitAfterPreviousTry < timePassed was true, after "
                "append, len(actionsLog): {}".format(len(self.actionsLog)))
            return True, 0.0
        else:
            logger.debug(
                "timeToWaitAfterPreviousTry < timePassed was false, "
                "len(actionsLog): {}".format(len(self.actionsLog)))
            return False, timeToWaitAfterPreviousTry - timePassed

    def _trimActionsLog(self, now):
        while self.actionsLog and now - self.actionsLog[0] > self.windowSize:
            self.actionsLog = self.actionsLog[1:]

    def _defaultDelayFunction(self, numOfActions):
        """
        Default delay function that always returns the size of the window.
        It limits rate of action to one per window
        """
        return self.windowSize
