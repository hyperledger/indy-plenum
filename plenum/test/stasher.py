import time

from plenum.common.log import getlogger

logger = getlogger()


class Stasher:
    def __init__(self, queue, name: str = None):
        self.delayRules = set()
        self.queue = queue
        self.delayeds = []
        self.created = time.perf_counter()
        self.name = name

    def delay(self, tester):
        """
        Delay messages for operation `op` when msg sent by node `frm`

        :param tester: a callable that takes as an argument the item
            from the queue and returns a number of seconds it should be delayed
        """
        logger.debug("{} adding delay for {}".format(self.name, tester))
        self.delayRules.add(tester)

    def nodelay(self, tester):
        if tester in self.delayRules:
            self.delayRules.remove(tester)
        else:
            logger.debug("{} not present in {}".format(tester, self.name))

    def process(self, age: float = None):
        age = age if age is not None else time.perf_counter() - self.created
        self.stashAll(age)
        self.unstashAll(age)

    def stashAll(self, age):
        for tester in self.delayRules:
            for rx in list(self.queue):
                secondsToDelay = tester(rx)
                if secondsToDelay:
                    logger.debug("{} stashing message {} for "
                                  "{} seconds".
                                  format(self.name, rx, secondsToDelay))
                    self.delayeds.append((age + secondsToDelay, rx))
                    self.queue.remove(rx)

    def unstashAll(self, age):
        """
        Not terribly efficient, but for now, this is only used for testing.
        HasActionQueue is more efficient about knowing when to iterate through
        the delayeds.

        :param age: seconds since Stasher started
        """
        for d in self.delayeds:
            if age >= d[0]:
                logger.debug(
                        "{} unstashing message {} ({:.0f} milliseconds overdue)".
                            format(self.name, d[1], (age - d[0]) * 1000))
                self.queue.appendleft(d[1])
                self.delayeds.remove(d)

    def resetDelays(self):
        logger.debug("{} resetting delays".format(self.name))
        self.delayRules = set()


