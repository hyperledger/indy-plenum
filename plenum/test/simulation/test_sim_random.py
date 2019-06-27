from plenum.test.simulation.sim_random import DefaultSimRandom


def test_sim_random_generates_integers_in_required_range():
    rnd = DefaultSimRandom()
    values = [rnd.integer(10, 50) for _ in range(100)]
    assert all(10 <= v <= 50 for v in values)


def test_sim_random_chooses_values_from_required_set():
    rnd = DefaultSimRandom()
    source_values = ['some_value', 'other_value', 42]
    values = [rnd.choice(*source_values) for _ in range(100)]
    assert all(v in source_values for v in values)


def test_sim_random_is_deterministic():
    rnd1 = DefaultSimRandom(42)
    values1 = [rnd1.integer(10, 50) for _ in range(100)]

    rnd2 = DefaultSimRandom(42)
    values2 = [rnd2.integer(10, 50) for _ in range(100)]

    assert values1 == values2
