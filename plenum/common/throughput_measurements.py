from abc import ABCMeta, abstractmethod
from enum import unique, Enum

from common.exceptions import LogicError


class ThroughputMeasurement(metaclass=ABCMeta):
    """
    Measures request ordering throughput
    """

    @abstractmethod
    def init_time(self, start_ts):
        pass

    @abstractmethod
    def add_request(self, ordered_ts):
        pass

    @abstractmethod
    def get_throughput(self, request_time):
        return 0.0


class EMAThroughputMeasurement(ThroughputMeasurement):
    """
    Measures request ordering throughput using exponential moving average
    """

    def __init__(self, window_size=15, min_cnt=16):
        """
        Creates a throughput measurement instance.

        :param window_size: window size in seconds for each next re-calculation
        of throughput
        :param min_cnt: N - the count of last calculated values which
        contribute with significant weight in the next value being calculated
        (approximately 86% in total)
        """
        self.reqs_in_window = 0
        self.throughput = 0
        self.window_size = window_size
        self.min_cnt = min_cnt
        self.alpha = 2 / (self.min_cnt + 1)
        self.window_start_ts = None

    def init_time(self, start_ts):
        self.window_start_ts = start_ts

    def add_request(self, ordered_ts):
        self._update_time(ordered_ts)
        self.reqs_in_window += 1

    def _accumulate(self, old_accum, next_val):
        """
        Implement exponential moving average
        """
        return old_accum * (1 - self.alpha) + next_val * self.alpha

    def _process_window(self):
        self.throughput = self._accumulate(self.throughput, self.reqs_in_window / self.window_size)

    def _update_time(self, current_ts):
        while current_ts >= self.window_start_ts + self.window_size:
            self._process_window()
            self.window_start_ts += self.window_size
            self.reqs_in_window = 0

    def get_throughput(self, request_time):
        self._update_time(request_time)
        return self.throughput


class SafeStartEMAThroughputMeasurement(EMAThroughputMeasurement):
    """
    Measures request ordering throughput using exponential moving average and
    also has a safe period on start consisting of `min_cnt` windows when None
    is returned instead of a calculated value. This can be useful for leveling
    instances on start when request ordering is just beginning.
    """

    def __init__(self, *args, **kwargs):
        """
        Creates a throughput measurement instance.
        """
        super().__init__(*args, **kwargs)
        self.first_ts = None

    def init_time(self, start_ts):
        super().init_time(start_ts)
        self.first_ts = start_ts

    def get_throughput(self, request_time):
        if request_time < self.first_ts + (self.window_size * self.min_cnt):
            return None
        return super().get_throughput(request_time)


class RevivalSpikeResistantEMAThroughputMeasurement(SafeStartEMAThroughputMeasurement):
    """
    Measures request ordering throughput using exponential moving average but
    is resistant to spikes after long idles (fade-outs). This can be useful for
    getting rid of treating a spike of queued requests on a backup instance
    after the primary reconnection as a master degradation indicator which is
    false positive in this case. Fade-out period length (after which requests
    are interpreted as a revival spike) and revival period length (during which
    None is returned instead of a calculated value) are `min_cnt` windows.
    """

    @unique
    class State(Enum):
        NORMAL = 0
        IDLE = 1
        FADED = 2
        REVIVAL = 3

    def __init__(self, *args, **kwargs):
        """
        Creates a throughput measurement instance.
        """
        super().__init__(*args, **kwargs)
        self.state = self.State.FADED

        # Fields being used in IDLE, FADED and REVIVAL states
        self.throughput_before_idle = 0
        self.idle_start_ts = None  # will be initialized in `init_time`
        self.empty_windows_count = 0

        # Fields being used in REVIVAL state only
        self.revival_start_ts = None
        self.revival_windows_count = None
        self.reqs_during_revival = None

    def init_time(self, start_ts):
        super().init_time(start_ts)
        self.idle_start_ts = start_ts

    def _process_window_in_normal_mode(self):
        if self.reqs_in_window == 0:
            self.state = self.State.IDLE
            self.throughput_before_idle = self.throughput
            self.idle_start_ts = self.window_start_ts
            self.empty_windows_count = 1

        window_reqs_rate = self.reqs_in_window / self.window_size
        self.throughput = self._accumulate(self.throughput, window_reqs_rate)

    def _process_window_in_idle_mode(self):
        if self.reqs_in_window == 0:
            self.empty_windows_count += 1
            if self.empty_windows_count == self.min_cnt:
                self.state = self.State.FADED
        else:
            self.state = self.State.NORMAL

        window_reqs_rate = self.reqs_in_window / self.window_size
        self.throughput = self._accumulate(self.throughput, window_reqs_rate)

    def _process_window_in_faded_mode(self):
        if self.reqs_in_window == 0:
            self.empty_windows_count += 1
            self.throughput = self._accumulate(self.throughput, 0)
        else:
            self.state = self.State.REVIVAL
            self.revival_start_ts = self.window_start_ts
            self.revival_windows_count = 1
            self.reqs_during_revival = self.reqs_in_window
            self.throughput = None

    def _level_reqs_after_revival(self):
        leveling_windows_count = \
            self.empty_windows_count + self.revival_windows_count
        leveled_reqs_per_window = \
            self.reqs_during_revival / leveling_windows_count
        leveled_reqs_rate = leveled_reqs_per_window / self.window_size

        self.throughput = self.throughput_before_idle
        for i in range(leveling_windows_count):
            self.throughput = \
                self._accumulate(self.throughput, leveled_reqs_rate)

    def _process_window_in_revival_mode(self):
        if self.reqs_in_window > 0:
            self.revival_windows_count += 1
            self.reqs_during_revival += self.reqs_in_window
            if self.revival_windows_count == self.min_cnt:
                self._level_reqs_after_revival()
                self.state = self.State.NORMAL
        else:
            self._level_reqs_after_revival()
            self.state = self.State.IDLE
            self.throughput_before_idle = self.throughput
            self.idle_start_ts = self.window_start_ts
            self.empty_windows_count = 1
            self.throughput = self._accumulate(self.throughput, 0)

    def _process_window(self):
        if self.state == self.State.NORMAL:
            self._process_window_in_normal_mode()
        elif self.state == self.State.IDLE:
            self._process_window_in_idle_mode()
        elif self.state == self.State.FADED:
            self._process_window_in_faded_mode()
        elif self.state == self.State.REVIVAL:
            self._process_window_in_revival_mode()
        else:
            raise LogicError("Internal state of throughput measurement {} "
                             "is unsupported".format(self.state))
