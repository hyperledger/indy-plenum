from abc import ABC
from statistics import median_low, median, median_high
from typing import List


class AverageStrategyBase(ABC):
    @staticmethod
    def get_avg(metrics: List):
        raise NotImplementedError()


class MedianLowStrategy(AverageStrategyBase):
    @staticmethod
    def get_avg(metrics: List):
        return median_low(metrics)


class MedianMediumStrategy(AverageStrategyBase):
    @staticmethod
    def get_avg(metrics: List):
        return median(metrics)


class MedianHighStrategy(AverageStrategyBase):
    @staticmethod
    def get_avg(metrics: List):
        return median_high(metrics)
