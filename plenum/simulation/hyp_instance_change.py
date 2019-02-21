from hypothesis import strategies as st, reproduce_failure
from hypothesis import given, settings

from plenum.simulation.pool_model import RestartEvent, OutageEvent, CorruptEvent, PoolModel
from plenum.simulation.sim_event_stream import ListEventStream, sim_events, ErrorEvent
from plenum.simulation.sim_model import ModelWithExternalEvents, process_model
from plenum.simulation.timer_model import TimerModel

settings.register_profile("big", buffer_size=128 * 1024, max_examples=1000)
settings.load_profile("big")


@st.composite
def restart_event(draw, node_names):
    return RestartEvent(node=draw(st.sampled_from(node_names)))


@st.composite
def outage_event(draw, max_count, node_names, min_duration=1, max_duration=10):
    node = draw(st.sampled_from(node_names))
    allowed_nodes = [n for n in node_names if n != node]
    st_disconnected = st.sampled_from(allowed_nodes)
    disconnecteds = draw(st.sets(elements=st_disconnected, min_size=1, max_size=max_count))
    duration = draw(st.integers(min_value=min_duration, max_value=max_duration))
    return OutageEvent(node=node, disconnecteds=disconnecteds, duration=duration)


@st.composite
def corrupt_event(draw, node_names):
    return CorruptEvent(node=draw(st.sampled_from(node_names)))


@st.composite
def pool_model_events(draw, node_count):
    greek_names = ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'Zeta', 'Eta', 'Theta']
    node_names = greek_names[:node_count]

    events = []
    events.extend(draw(sim_events(
        st.one_of(outage_event(max_count=1, node_names=node_names, min_duration=3),
                  outage_event(max_count=node_count // 2, node_names=node_names, min_duration=3),
                  restart_event(node_names=node_names)),
        min_interval=10
    )))
    events.extend(draw(sim_events(
        corrupt_event(node_names=node_names),
        max_size=1, min_interval=50, max_interval=1000
    )))

    pool = PoolModel(node_names)
    model = ModelWithExternalEvents(pool, ListEventStream(events))
    return pool, process_model(draw, model,
                               max_size=1000)


@reproduce_failure('4.6.0', b'AAEtAQEBAQAEAAEAAAAA')
@given(pool_events=pool_model_events(node_count=4))
def test_pool_model(pool_events):
    pool, events = pool_events
    assert pool.error_status() is None
    assert not any(isinstance(ev.payload, ErrorEvent) for ev in events)
