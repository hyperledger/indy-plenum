import time

from stp_core.common.log import getlogger

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

    def unstashAll(self, age, *names, ignore_age_check=False):
        """
        Not terribly efficient, but for now, this is only used for testing.
        HasActionQueue is more efficient about knowing when to iterate through
        the delayeds.

        :param age: seconds since Stasher started
        """
        unstashed = 0
        to_remove = []
        for idx, d in enumerate(self.delayeds):
            # This is in-efficient as `ignore_age_check` wont change during loop
            # but its ok since its a testing util.
            if ignore_age_check or (
                    names and d[1][0].__name__ in names) or age >= d[0]:
                if ignore_age_check:
                    msg = '(forced)'
                elif names and d[1][0].__name__ in names:
                    msg = '({} present in {})'.format(d[1][0].__name__, names)
                else:
                    msg = '({:.0f} milliseconds overdue)'.format(
                        (age - d[0]) * 1000)
                logger.debug(
                    "{} unstashing message {} {}".
                    format(self.name, d[1], msg))
                self.queue.appendleft(d[1])
                to_remove.append(idx)
                unstashed += 1

        # Since `to_remove` is filled with increasing numbers so reverse it
        # and then remove elements from list
        for idx in to_remove[::-1]:
            self.delayeds.pop(idx)

        return unstashed

    def resetDelays(self, *names):
        if not names:
            logger.debug("{} resetting all delays".format(self.name))
            self.delayRules = set()
        else:
            logger.debug("{} resetting delays for {}".format(self.name, names))
            to_remove = []
            for r in self.delayRules:
                if r.__name__ in names:
                    to_remove.append(r)

            for r in to_remove:
                self.delayRules.remove(r)

    def drop_delayeds(self):
        # This will empty the stashed message queue
        self.delayeds = []

    def force_unstash(self, *names):
        if not names:
            return self.unstashAll(0, ignore_age_check=True)
        else:
            return self.unstashAll(0, *names)

    def reset_delays_and_process_delayeds(self, *names):
        self.resetDelays(*names)
        self.force_unstash(*names)
