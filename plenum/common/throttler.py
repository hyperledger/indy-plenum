import time


class Throttler:

    def __init__(self, windowSize, delayFunction = None):
        '''
        Limits rate of actions performed in a unit of time (window)

        :param windowSize: size (in seconds) of the time window events counted in
        :param delayFunction: function from **number of actions** to **time to wait after the last one**
        '''

        assert windowSize and windowSize > 0
        self.windowSize = windowSize
        self.delayFunction = delayFunction if delayFunction else self._defaultDelayFunction
        self.actionsLog = []

    def acquire(self):
        '''
        Acquires lock for action.

        :return: True and 0.0 if lock successfully acquired or False and number of seconds to wait before the next try
        '''
        now = time.perf_counter()
        self._trimActionsLog(now)
        if len(self.actionsLog) == 0:
            self.actionsLog.append(now)
            return True, 0.0
        timeToWaitAfterPreviousTry = self.delayFunction(len(self.actionsLog))
        timePassed = now - self.actionsLog[-1]
        if timeToWaitAfterPreviousTry < timePassed:
            self.actionsLog.append(now)
            return True, 0.0
        else:
            return False, timeToWaitAfterPreviousTry - timePassed

    def _trimActionsLog(self, now):
        while self.actionsLog and now - self.actionsLog[0] > self.windowSize:
            self.actionsLog = self.actionsLog[1:]

    def _defaultDelayFunction(self, numOfActions):
        '''
        Default delay function that always returns the size of the window.
        It limits rate of action to one per window
        '''
        return self.windowSize
