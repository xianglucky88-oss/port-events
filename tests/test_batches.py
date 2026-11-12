import json
import sqlite3

import pytest

from port_events.connectors import JsonEventSource, SourceBatch
from port_events.pipeline import PortEventsPipeline
from port_events.storage import EventStore


class CrashError(BaseException):
    """模拟进程崩溃：不被管道按批次捕获，直接中断本轮执行。"""


class CrashBeforeCommitStore(EventStore):
    def commit_batch(self, source_id, batch_id, events):
        raise CrashError("process died before commit")


class CrashDuringCommitStore(EventStore):
    def commit_batch(self, source_id, batch_id, events):
        self.connection.execute("BEGIN IMMEDIATE")
        self._merge_upsert(events)
        self.connection.close()  # 进程消亡：未提交事务随连接关闭回滚
        raise CrashError("process died during commit")


class CrashAfterCommitStore(EventStore):
    def commit_batch(self, source_id, batch_id, events):
        super().commit_batch(source_id, batch_id, events)
        raise CrashError("process died after commit")


def carrier_record(**overrides):
    record = {
        "source": "carrier-api",
        "vessel": "Ocean Star",
        "voyage": "OS202611A",
        "container": "CONT001",
        "port": "CNSHA",
        "event_code": "ATA",
        "event_time": "2026-11-10T08:00:00+08:00",
        "status": "estimated",
        "source_ref": "carrier-event-1",
    }
    record.update(overrides)
    return record


def terminal_record(**overrides):
    record = {
        "source": "terminal-api",
        "vessel": "ocean star",
        "voyage": "OS202611A",
        "container": "CONT001",
        "port": "cnsha",
        "event_code": "ATA",
        "event_time": "2026-11-10T00:00:00Z",
        "status": "actual",
        "source_ref": "terminal-event-7",
    }
    record.update(overrides)
    return record


def depart_record(**overrides):
    return carrier_record(
        event_code="DEPART",
        event_time="2026-11-12T18:30:00+08:00",
        source_ref="carrier-event-2",
        **overrides,
    )


def write_json(path, records):
    path.write_text(json.dumps(records), encoding="utf-8")


def make_batches(work, carrier_records, terminal_records,
                 carrier_batch="carrier-b1", terminal_batch="terminal-b1"):
    carrier_path = work / "carrier.json"
    terminal_path = work / "terminal.json"
    if carrier_records is not None:
        write_json(carrier_path, carrier_records)
    if terminal_records is not None:
        write_json(terminal_path, terminal_records)
    return (
        SourceBatch("carrier", carrier_batch, JsonEventSource(carrier_path)),
        SourceBatch("terminal", terminal_batch, JsonEventSource(terminal_path)),
    )


def key_fields(events):
    return [(e.event_key, e.status, e.sources) for e in events]


def test_carrier_file_missing_terminal_available_and_failed_batch_retried(tmp_path):
    # 承运人文件暂不可读：码头本轮事件仍须可用
    carrier, terminal = make_batches(tmp_path, None, [terminal_record()])
    store = EventStore(tmp_path / "events.sqlite3")
    pipeline = PortEventsPipeline(None, store)

    first = pipeline.run_batches([carrier, terminal])
    assert [o.status for o in first.outcomes] == ["failed", "committed"]
    assert "read failed" in first.failed[0].error
    events = store.fetch_events()
    assert len(events) == 1
    assert events[0].sources == ("terminal-event-7",)
    # 失败状态持久化，重启后可识别待执行批次
    assert [(b.source_id, b.state) for b in store.retryable_batches()] == [("carrier", "failed")]

    # 恢复后重跑：只重试失败批次，已提交批次跳过
    write_json(tmp_path / "carrier.json", [carrier_record()])
    second = pipeline.run_batches([carrier, terminal])
    assert [o.status for o in second.outcomes] == ["committed", "skipped"]
    events = store.fetch_events()
    assert len(events) == 1
    assert events[0].status == "actual"
    assert events[0].sources == ("carrier-event-1", "terminal-event-7")
    assert store.retryable_batches() == []
    store.close()


def test_invalid_record_fails_whole_batch_other_source_commits(tmp_path):
    carrier, terminal = make_batches(
        tmp_path,
        [carrier_record(), carrier_record(event_code="FLY", source_ref="carrier-event-9")],
        [terminal_record()],
    )
    store = EventStore(tmp_path / "events.sqlite3")
    pipeline = PortEventsPipeline(None, store)

    report = pipeline.run_batches([carrier, terminal])
    assert [o.status for o in report.outcomes] == ["failed", "committed"]
    assert "invalid record" in report.failed[0].error
    # 整批失败：carrier 一条也不写入
    events = store.fetch_events()
    assert len(events) == 1
    assert events[0].sources == ("terminal-event-7",)

    # 修复文件后整批重试成功
    write_json(tmp_path / "carrier.json", [carrier_record(), depart_record()])
    report = pipeline.run_batches([carrier, terminal])
    assert [o.status for o in report.outcomes] == ["committed", "skipped"]
    assert report.committed[0].event_count == 2
    assert len(store.fetch_events()) == 2
    assert store.retryable_batches() == []
    store.close()


def test_replay_same_batch_skipped_new_batch_merges_by_event_key(tmp_path):
    carrier, terminal = make_batches(tmp_path, [carrier_record()], [terminal_record()])
    store = EventStore(tmp_path / "events.sqlite3")
    pipeline = PortEventsPipeline(None, store)
    pipeline.run_batches([carrier, terminal])
    before = store.fetch_events()

    # 同批重放：即使文件内容变化，批次标识不变一律跳过
    write_json(tmp_path / "carrier.json", [carrier_record(status="actual", source_ref="carrier-event-9")])
    replay = pipeline.run_batches([carrier, terminal])
    assert [o.status for o in replay.outcomes] == ["skipped", "skipped"]
    assert store.fetch_events() == before

    # 新批次：同一精确键按既有规则合并（actual 优先、来源取并集）
    new_batch = SourceBatch("carrier", "carrier-b2", JsonEventSource(tmp_path / "carrier.json"))
    applied = pipeline.run_batches([new_batch])
    assert [o.status for o in applied.outcomes] == ["committed"]
    (event,) = store.fetch_events()
    assert event.status == "actual"
    assert event.sources == ("carrier-event-1", "carrier-event-9", "terminal-event-7")
    store.close()


def test_final_state_independent_of_batch_and_record_order(tmp_path):
    final_states = []
    for label in ("forward", "reverse"):
        work = tmp_path / label
        work.mkdir()
        carrier_records = [carrier_record(), depart_record()]
        if label == "reverse":
            carrier_records.reverse()
        carrier, terminal = make_batches(work, carrier_records, [terminal_record()])
        store = EventStore(work / "events.sqlite3")
        batches = [carrier, terminal] if label == "forward" else [terminal, carrier]
        PortEventsPipeline(None, store).run_batches(batches)
        final_states.append(key_fields(store.fetch_events()))
        store.close()
    assert len(final_states[0]) == 2
    assert final_states[0] == final_states[1]


def test_crash_before_commit_leaves_batch_pending_and_recovers(tmp_path):
    carrier, terminal = make_batches(tmp_path, [carrier_record()], [terminal_record()])
    store = CrashBeforeCommitStore(tmp_path / "events.sqlite3")
    with pytest.raises(CrashError):
        PortEventsPipeline(None, store).run_batches([carrier, terminal])
    assert store.batch_state("carrier", "carrier-b1") == "pending"
    assert store.fetch_events() == []
    store.close()

    # 重启：待执行批次被识别并补做，且只补做未提交批次
    store = EventStore(tmp_path / "events.sqlite3")
    assert [b.batch_id for b in store.retryable_batches()] == ["carrier-b1"]
    report = PortEventsPipeline(None, store).run_batches([carrier, terminal])
    assert [o.status for o in report.outcomes] == ["committed", "committed"]
    assert len(store.fetch_events()) == 1
    assert store.retryable_batches() == []
    store.close()


def test_crash_during_commit_rolls_back_and_recovers(tmp_path):
    carrier, terminal = make_batches(tmp_path, [carrier_record()], [terminal_record()])
    store = CrashDuringCommitStore(tmp_path / "events.sqlite3")
    with pytest.raises(CrashError):
        PortEventsPipeline(None, store).run_batches([carrier, terminal])

    # 提交中崩溃：事件与批次确认一起回滚，重启后重试
    store = EventStore(tmp_path / "events.sqlite3")
    assert store.fetch_events() == []
    assert [(b.source_id, b.state) for b in store.retryable_batches()] == [("carrier", "pending")]
    report = PortEventsPipeline(None, store).run_batches([carrier, terminal])
    assert [o.status for o in report.outcomes] == ["committed", "committed"]
    assert len(store.fetch_events()) == 1
    store.close()


def test_crash_after_commit_is_not_replayed(tmp_path):
    carrier, terminal = make_batches(tmp_path, [carrier_record()], [terminal_record()])
    store = CrashAfterCommitStore(tmp_path / "events.sqlite3")
    with pytest.raises(CrashError):
        PortEventsPipeline(None, store).run_batches([carrier, terminal])
    store.close()

    # 提交后崩溃：日志即事实，重启后跳过已提交批次，不重放
    store = EventStore(tmp_path / "events.sqlite3")
    assert store.batch_state("carrier", "carrier-b1") == "committed"
    report = PortEventsPipeline(None, store).run_batches([carrier, terminal])
    assert [o.status for o in report.outcomes] == ["skipped", "committed"]
    events = store.fetch_events()
    assert len(events) == 1
    assert events[0].sources == ("carrier-event-1", "terminal-event-7")
    store.close()


def test_lock_conflict_marks_batches_retryable_and_recovers(tmp_path):
    carrier, terminal = make_batches(tmp_path, [carrier_record()], [terminal_record()])
    db = tmp_path / "events.sqlite3"
    store = EventStore(db, timeout=0.05)
    blocker = sqlite3.connect(db, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    try:
        report = PortEventsPipeline(None, store).run_batches([carrier, terminal])
        assert [o.status for o in report.outcomes] == ["failed", "failed"]
        assert all("locked" in o.error for o in report.failed)
        assert store.fetch_events() == []
    finally:
        blocker.execute("ROLLBACK")
        blocker.close()

    # 锁释放后重试成功
    report = PortEventsPipeline(None, store).run_batches([carrier, terminal])
    assert [o.status for o in report.outcomes] == ["committed", "committed"]
    assert len(store.fetch_events()) == 1
    assert store.retryable_batches() == []
    store.close()
