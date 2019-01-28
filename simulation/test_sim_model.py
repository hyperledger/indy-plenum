from typing import List

from hypothesis import given
from hypothesis import strategies as st

from helper import MAX_EVENTS_SIZE, some_events, check_event_stream_invariants, some_event
from sim_event_stream import SimEvent, ErrorEvent, sim_event_stream, ListEventStream
from sim_model import SimModel, ModelEventStream


@st.composite
def model_event_stream(draw, model_factory):
    input_a = draw(some_events(max_size=MAX_EVENTS_SIZE // 2))
    input_b = draw(some_events(max_size=MAX_EVENTS_SIZE // 2))
    stream_a = ListEventStream(input_a)
    stream_b = ListEventStream(input_b)
    model = model_factory()
    stream = ModelEventStream(draw, model, stream_a, stream_b)
    events = draw(sim_event_stream(stream, max_size=MAX_EVENTS_SIZE))
    return input_a, input_b, events, model


class PassiveModel(SimModel):
    def __init__(self):
        self.processed_events = []

    def process(self, draw, event: SimEvent, is_stable: bool) -> List[SimEvent]:
        self.processed_events.append(event)
        return []


@given(inputs_events_model=model_event_stream(PassiveModel))
def test_passive_model_properties(inputs_events_model):
    input_a, input_b, events, model = inputs_events_model
    check_event_stream_invariants(events)

    # Model should have seen all events
    assert model.processed_events == events

    # All input events should be present in generated events
    assert all(ev in events for ev in input_a)
    assert all(ev in events for ev in input_b)


class RandomErrorModel(SimModel):
    def __init__(self):
        self.processed_events = []
        self._events = st.one_of(st.just(ErrorEvent(reason="random")),
                                 some_event())

    def process(self, draw, event: SimEvent, is_stable: bool) -> List[SimEvent]:
        self.processed_events.append(event)
        ts = event.timestamp
        delays = draw(st.lists(elements=st.integers(min_value=0, max_value=1000)))
        events = [SimEvent(timestamp=ts + delay, payload=draw(self._events)) for delay in delays]
        return events


@given(inputs_events_model=model_event_stream(RandomErrorModel))
def test_random_error_model_properties(inputs_events_model):
    input_a, input_b, events, model = inputs_events_model
    check_event_stream_invariants(events)

    # Model should have seen all events except own generated error
    if any(isinstance(ev.payload, ErrorEvent) for ev in events):
        assert model.processed_events == events[:-1]
    else:
        assert model.processed_events == events
