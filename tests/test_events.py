from port_events.connectors import JsonEventSource
from port_events.pipeline import PortEventsPipeline
from port_events.storage import EventStore


def test_events_are_correlated_and_actual_wins(tmp_path):
    source_path = tmp_path / "events.json"
    source_path.write_text('[{"source":"a","vessel":"V","voyage":"1","container":"C","port":"P","event_code":"ATA","event_time":"2026-01-01T00:00:00Z","status":"estimated","source_ref":"a1"},{"source":"b","vessel":"v","voyage":"1","container":"C","port":"p","event_code":"ATA","event_time":"2025-12-31T16:00:00-08:00","status":"actual","source_ref":"b1"}]')
    store = EventStore(tmp_path / "events.sqlite3")
    try:
        events = PortEventsPipeline(JsonEventSource(source_path), store).run()
        assert len(events) == 1
        assert events[0].status == "actual"
        assert events[0].sources == ("a1", "b1")
    finally:
        store.close()
