from hypothesis import strategies as st, given

from plenum.hypothesis.helper import HypothesisSimRandom
from plenum.test.consensus.test_sim_view_change import check_view_change_completes_under_normal_conditions


@given(st.data())
def test_view_change_completes_under_normal_conditions(data):
    random = HypothesisSimRandom(data)
    check_view_change_completes_under_normal_conditions(random)
