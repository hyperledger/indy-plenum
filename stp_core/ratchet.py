import functools
from math import exp, log


class Ratchet:
    def __init__(self, a: float, b: float, c: float=0,
                 base: float=None, peak: float=None):
        """
        Models an exponential curve; useful for providing the number of seconds
        to wait between retries

        :param a: multiplier
        :param b: exponent multiplier
        :param c: offset
        :param base: minimum number returned
        :param peak: maximum number returned
        """
        self.a = a
        self.b = b
        self.c = c
        self.base = base
        self.peak = peak

    @classmethod
    def fromGoals(cls, start: float, end: float, steps: int):
        b = log(end / start) / (steps - 1)
        return cls(a=start, b=b)

    @staticmethod
    def _sumSeries(a: float, b: float, steps: int) -> float:
        """
        Return value of the the following polynomial.
        .. math::
           (a * e^(b*steps) - 1) / (e^b - 1)

        :param a: multiplier
        :param b: exponent multiplier
        :param steps: the number of steps
        """
        return a * (exp(b * steps) - 1) / (exp(b) - 1)

    @classmethod
    def fromGoalDuration(cls, start, steps, total):
        return cls(a=start, b=Ratchet.goalDuration(start, steps, total))

    @staticmethod
    @functools.lru_cache()
    def goalDuration(start: float, steps: int, total: float) -> float:
        """
        Finds a b-value (common ratio) that satisfies a total duration within
        1 millisecond. Not terribly efficient, so using lru_cache. Don't know
        a way to compute the common ratio when the sum of a finite geometric
        series is known. Found myself needing to factor polynomials with an
        arbitrarily
        high degree.

        :param start: a-value
        :param steps: how many steps
        :param total: total duration of the series of n-steps
        :return: b value
        """
        a = start
        up = None
        dn = None
        b = 1.0
        while True:
            s = Ratchet._sumSeries(a, b, steps) - total
            if abs(s) < .001:
                break
            elif s < 0:
                dn = b
                # halfway between b and upper if upper defined
                b = (up + b) / 2 if up else b + 1
            else:
                up = b
                b = (dn + b) / 2 if dn else b / 2
        return b

    def get(self, iteration: int):
        v = (self.a * exp(self.b * iteration)) + self.c
        v = max(self.base, v) if self.base else v
        v = min(self.peak, v) if self.peak else v
        return v

    def gen(self):
        i = 0
        while True:
            newI = yield self.get(i)
            if newI is not None:
                i = newI
            else:
                i += 1
