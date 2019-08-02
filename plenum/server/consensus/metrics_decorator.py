import functools

from plenum.common.metrics_collector import MetricsName


def measure_consensus_time(master_name: MetricsName, backup_name: MetricsName):
    def decorator(f):
        @functools.wraps(f)
        def wrapper(self, *args, **kwargs):
            metrics = self.metrics
            if self.is_master:
                with metrics.measure_time(master_name):
                    return f(self, *args, **kwargs)
            else:
                with metrics.measure_time(backup_name):
                    return f(self, *args, **kwargs)

        return wrapper

    return decorator
