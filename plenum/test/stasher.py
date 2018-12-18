import time
from collections import namedtuple
from contextlib import contextmanager

from stp_core.common.log import getlogger

logger = getlogger()

StasherDelayed = namedtuple('StasherDelayed', 'item timestamp rule')


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

        Note: current reliance on tester.__name__ to remove particular
        delay rules could lead to problems when adding testers with
        same function but different parameters.
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
                    logger.info("{} stashing message {} for "
                                 "{} seconds".
                                 format(self.name, rx, secondsToDelay))
                    self.delayeds.append(StasherDelayed(item=rx, timestamp=age + secondsToDelay, rule=tester.__name__))
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
                    names and d.rule in names) or age >= d.timestamp:
                if ignore_age_check:
                    msg = '(forced)'
                elif names and d.rule in names:
                    msg = '({} present in {})'.format(d.rule, names)
                else:
                    msg = '({:.0f} milliseconds overdue)'.format(
                        (age - d.timestamp) * 1000)
                logger.info(
                    "{} unstashing message {} {}".
                        format(self.name, d.item, msg))
                self.queue.append(d.item)
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
        """
        Remove delay rules and unstash related messages.

        :param names: list of delay functions names to unstash

        Note that original implementation made an assumption that messages are tuples
        and relied on first element __name__ to find messages to unstash, but new one
        explicitly stores name of delay rule function when stashing messages. Also
        most delay rule functions override their __name__ to match delayed message.
        While usages looking like reset_delays_and_process_delayeds(COMMIT) won't break
        as long as last assumption holds true it's still recommended to consider using
        new context manager where applicable to reduce potential errors in tests.
        """
        self.resetDelays(*names)
        self.force_unstash(*names)


@contextmanager
def delay_rules(stasher, *delayers):
    """
    Context manager to add delay rules to stasher(s) on entry and clean everything up on exit.

    :param stasher: Instance of Stasher or iterable over instances of stasher
    :param delayers: Delay rule functions to be added to stashers
    """
    try:
        stashers = [s for s in stasher]
    except TypeError:
        stashers = [stasher]

    for s in stashers:
        if not isinstance(s, Stasher):
            raise TypeError("expected Stasher or Iterable[Stasher] as a first argument")

    for s in stashers:
        for d in delayers:
            s.delay(d)
    yield
    for s in stashers:
        s.reset_delays_and_process_delayeds(*(d.__name__ for d in delayers))
