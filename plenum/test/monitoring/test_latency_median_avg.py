def test_low_median_avg_latencies(tconf):
    master_lat = 120
    # Filling for case, when the most of backup latencies are small and several is very big
    # Case is [120, 101, 1200, 1200, 1200]
    latencies = [120, 101, 1200, 1200, 1200]
    avg_strategy = tconf.LatencyAveragingStrategyClass()
    assert master_lat - avg_strategy.get_avg(latencies) < tconf.OMEGA


def test_medium_median_avg_latencies(tconf):
    master_lat = 120
    # Filling for case, when the most of backup latencies are similar with master and several is very big
    # Case is [120, 1, 101, 101, 1200]
    latencies = [120, 1, 101, 101, 1200]
    avg_strategy = tconf.LatencyAveragingStrategyClass()
    assert master_lat - avg_strategy.get_avg(latencies) < tconf.OMEGA


def test_high_median_avg_latencies(tconf):
    master_lat = 120
    # Filling for case, when the most of backup latencies a similar with master,
    # but there is a some very big values
    # Case is [120, 1, 101, 101, 1]
    latencies = [120, 1, 101, 101, 1]
    avg_strategy = tconf.LatencyAveragingStrategyClass()
    assert master_lat - avg_strategy.get_avg(latencies) < tconf.OMEGA


def test_trigger_view_change(tconf):
    # Filling for case, when the master's average latency is not acceptable
    # Case is [120, 99, 99, 99, 99]
    master_lat = 120
    latencies = [120, 99, 99, 99, 99]
    avg_strategy = tconf.LatencyAveragingStrategyClass()
    assert master_lat - avg_strategy.get_avg(latencies) > tconf.OMEGA
