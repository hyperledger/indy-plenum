from plenum.test.simulation.sim_random import DefaultSimRandom, SimRandom


def test_sim_random_generates_integers_in_required_range():
    rnd = DefaultSimRandom()
    values = [rnd.integer(10, 50) for _ in range(100)]
    assert all(10 <= v <= 50 for v in values)


def test_sim_random_chooses_values_from_required_set():
    rnd = DefaultSimRandom()
    source_values = ['some_value', 'other_value', 42]
    values = [rnd.choice(*source_values) for _ in range(100)]
    assert all(v in source_values for v in values)


def test_sim_random_samples_values_from_required_set():
    rnd = DefaultSimRandom()
    source_values = ['some_value', 'other_value', 42, 'another_value', 73]
    values = rnd.sample(source_values, 3)
    assert len(values) == 3
    assert len(set(values)) == 3
    assert all(v in source_values for v in values)


def test_sim_random_shuffles():
    rnd = DefaultSimRandom()
    values = [rnd.integer(10, 50) for _ in range(100)]
    shuffled = rnd.shuffle(values)
    assert values != shuffled


def test_sim_random_is_deterministic():
    def generate_some(rnd: SimRandom):
        values = [rnd.integer(-37, 7342) for _ in range(1000)]
        choice = rnd.choice(*values)
        sample = rnd.sample(values, 10)
        shuffled = rnd.shuffle(values)
        return values, choice, sample, shuffled

    rnd1 = DefaultSimRandom(42)
    rnd2 = DefaultSimRandom(42)
    other_rnd = DefaultSimRandom(73)

    data1 = generate_some(rnd1)
    data2 = generate_some(rnd2)
    other_data = generate_some(other_rnd)

    assert data1 == data2
    assert data1 != other_data


def test_sim_random_is_repeatable_between_runs():
    rnd = DefaultSimRandom()
    values = [rnd.integer(0, 100) for _ in range(5)]
    assert values == [49, 97, 53, 5, 33]
