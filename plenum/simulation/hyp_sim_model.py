from typing import Optional

from hypothesis import given
from hypothesis import strategies as st

from plenum.simulation.helper import some_events, MAX_EVENTS_SIZE, check_event_stream_invariants, some_event
from plenum.simulation.sim_event_stream import ListEventStream, sim_event_stream, SimEvent, ErrorEvent, \
    CompositeEventStream
from plenum.simulation.sim_model import SimModel, process_model, ModelWithExternalEvents


@st.composite
def model_event_stream(draw, model_factory):
    input_a = draw(some_events(max_size=MAX_EVENTS_SIZE // 2))
    input_b = draw(some_events(max_size=MAX_EVENTS_SIZE // 2))
    stream_a = ListEventStream(input_a)
    stream_b = ListEventStream(input_b)
    stream = CompositeEventStream(stream_a, stream_b)
    model = model_factory()
    model_with_events = ModelWithExternalEvents(model, stream)
    events = process_model(draw, model_with_events, max_size=MAX_EVENTS_SIZE)
    return input_a, input_b, events, model


class PassiveModel(SimModel):
    def __init__(self):
        self.processed_events = []
        self._outbox = ListEventStream()

    def process(self, draw, event: SimEvent):
        self.processed_events.append(event)

    def outbox(self):
        return self._outbox

    def error_status(self) -> Optional[str]:
        pass


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
        self._outbox = ListEventStream()
        self._error_status = None

    def process(self, draw, event: SimEvent):
        self.processed_events.append(event)
        if isinstance(event.payload, ErrorEvent):
            self._error_status = 'Has error'
        ts = event.timestamp
        delays = draw(st.lists(elements=st.integers(min_value=1, max_value=1000)))
        self._outbox.extend(SimEvent(timestamp=ts + delay, payload=draw(self._events)) for delay in delays)

    def outbox(self):
        return self._outbox

    def error_status(self) -> Optional[str]:
        pass


@given(inputs_events_model=model_event_stream(RandomErrorModel))
def test_random_error_model_properties(inputs_events_model):
    input_a, input_b, events, model = inputs_events_model
    check_event_stream_invariants(events)

    # Model should have seen all events except own generated error
    if any(isinstance(ev.payload, ErrorEvent) for ev in events):
        assert model.processed_events == events[:-1]
    else:
        assert model.processed_events == events
