from hypothesis import strategies as st
from hypothesis import given, settings

from pool_model import PoolModel, RestartEvent, OutageEvent, CorruptEvent
from sim_event_stream import ErrorEvent, sim_event_stream, sim_events, ListEventStream, SimEvent
from sim_model import ModelEventStream

settings.register_profile("big", buffer_size=128 * 1024, max_examples=1000)
settings.load_profile("big")


@st.composite
def restart_event(draw, min_id, max_id):
    return RestartEvent(node_id=draw(st.integers(min_value=min_id, max_value=max_id)))


@st.composite
def outage_event(draw, max_count, min_id, max_id, min_duration=1, max_duration=10):
    node_id = draw(st.integers(min_value=min_id, max_value=max_id))
    st_disconnected_id = st.integers(min_value=min_id, max_value=max_id - 1)
    st_disconnected_ids = st.sets(elements=st_disconnected_id, min_size=1, max_size=max_count)
    disconnected_ids = draw(st_disconnected_ids)
    disconnected_ids = {v if v < node_id else v + 1 for v in disconnected_ids}
    duration = draw(st.integers(min_value=min_duration, max_value=max_duration))
    return OutageEvent(node_id=node_id, disconnected_ids=disconnected_ids, duration=duration)


@st.composite
def corrupt_event(draw, min_id, max_id):
    return CorruptEvent(node_id=draw(st.integers(min_value=min_id, max_value=max_id)))


@st.composite
def pool_model_events(draw, node_count):
    model = PoolModel(node_count)
    input_events = st.one_of(outage_event(max_count=1, min_id=1, max_id=node_count, min_duration=3),
                             outage_event(max_count=node_count // 2, min_id=1, max_id=node_count, min_duration=3),
                             restart_event(min_id=1, max_id=node_count))
    corrupt_events = corrupt_event(min_id=1, max_id=node_count)
    stream = ModelEventStream(draw, model,
                              ListEventStream(draw(sim_events(input_events, min_interval=10))),
                              ListEventStream(draw(sim_events(corrupt_events, max_size=1,
                                                              min_interval=50, max_interval=1000))))

    events = draw(sim_event_stream(stream, max_size=100))
    return events


@st.composite
def pool_model_deterministic(draw, node_count):
    model = PoolModel(node_count)
    input_events = ListEventStream([SimEvent(timestamp=10, payload=OutageEvent(node_id=1, disconnected_ids={2}, duration=3))])
    stream = ModelEventStream(draw, model, input_events)
    events = draw(sim_event_stream(stream, max_size=100))
    return events


@given(events=pool_model_events(node_count=4))
def test_pool_model(events):
    assert not any(isinstance(ev.payload, ErrorEvent) for ev in events)
