"""只读近似节点候选识别的验收测试。

覆盖任务要求的全部场景：跨航次接近、相邻同类、缺箱号桥接、边界距离、
重复来源、空库、不同插入序，并验证候选不重复、可回原行、纯只读。
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from itertools import permutations

import pytest

from port_events.candidates import find_node_candidates
from port_events.models import ShipmentEvent
from port_events.storage import EventStore, load_shipment_events_readonly


def make_event(
    event_key: str,
    *,
    vessel: str = "V1",
    voyage: str = "001",
    container: str | None = "C1",
    port: str = "CNSHA",
    event_type: str = "arrived",
    event_time_utc: str = "2026-11-10T00:00:00+00:00",
    status: str = "actual",
    sources: tuple[str, ...] = ("s1",),
) -> ShipmentEvent:
    return ShipmentEvent(
        event_key=event_key,
        vessel=vessel,
        voyage=voyage,
        container=container,
        port=port,
        event_type=event_type,
        event_time_utc=event_time_utc,
        status=status,
        sources=sources,
        observed_at="2026-11-10T01:00:00+00:00",
    )


def shift(time_text: str, minutes: int) -> str:
    moment = datetime.fromisoformat(time_text)
    return (moment + timedelta(minutes=minutes)).isoformat()


BASE = "2026-11-10T00:00:00+00:00"
WINDOW = timedelta(hours=1)


def all_pairs(candidates):
    pairs = []
    for candidate in candidates:
        keys = list(candidate.event_keys)
        if len(keys) == 2:
            pairs.append((keys[0], keys[1]))
        else:
            for index in range(len(keys)):
                for other in range(index + 1, len(keys)):
                    pairs.append((keys[index], keys[other]))
    return pairs


def test_empty_input_returns_no_candidates():
    assert find_node_candidates([]) == []


def test_candidate_traceable_to_original_rows_and_has_full_key():
    left = make_event("k-left", sources=("a-1",))
    right = make_event("k-right", event_time_utc=shift(BASE, 10), sources=("b-2",))
    candidates = find_node_candidates([left, right], time_window=WINDOW)

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.event_keys == ("k-left", "k-right")
    member_keys = {member.event_key for member in candidate.members}
    assert member_keys == {"k-left", "k-right"}
    # 来源引用可追溯。
    assert candidate.source_refs == ("a-1", "b-2")
    # 候选身份完全由内容派生且稳定。
    assert candidate.candidate_id.startswith("cand-")
    again = find_node_candidates([right, left], time_window=WINDOW)
    assert again[0].candidate_id == candidate.candidate_id


def test_insertion_order_does_not_change_output():
    events = [
        make_event("k1", event_time_utc=BASE, sources=("s1",)),
        make_event("k2", event_time_utc=shift(BASE, 10), sources=("s2",)),
        make_event(
            "k3",
            container=None,
            event_time_utc=shift(BASE, 20),
            sources=("s3",),
        ),
    ]
    baseline = find_node_candidates(events, time_window=WINDOW)
    for permutation in permutations(events):
        assert find_node_candidates(list(permutation), time_window=WINDOW) == baseline
    # 输出按 event_key 内容排序，而非输入顺序。
    assert [candidate.event_keys for candidate in baseline] == sorted(
        candidate.event_keys for candidate in baseline
    )


def test_boundary_distance_inclusive_and_just_outside_excluded():
    on_boundary = make_event("on-boundary", event_time_utc=shift(BASE, 60))
    outside = make_event("outside", event_time_utc=shift(BASE, 61))

    inside_result = find_node_candidates(
        [make_event("base", event_time_utc=BASE), on_boundary],
        time_window=WINDOW,
    )
    assert len(inside_result) == 1
    reason = next(
        reason
        for reason in inside_result[0].reasons
        if reason.code == "time_within_window"
    )
    assert reason.detail.startswith("distance_seconds=3600")

    outside_result = find_node_candidates(
        [make_event("base", event_time_utc=BASE), outside],
        time_window=WINDOW,
    )
    assert outside_result == []


def test_cross_voyage_close_events_are_weak_pair_not_group():
    left = make_event("k-a", voyage="VY-A")
    right = make_event(
        "k-b",
        voyage="VY-B",
        event_time_utc=shift(BASE, 15),
    )
    candidates = find_node_candidates([left, right], time_window=WINDOW)

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.shape == "pair"
    assert candidate.linkage == "weak"
    assert {conflict.field for conflict in candidate.conflicts} == {
        "voyage",
        "event_time_utc",
    }
    reason_codes = {reason.code for reason in candidate.reasons}
    assert "voyage_differs" in reason_codes
    assert "same_port" in reason_codes and "same_event_type" in reason_codes
    assert "跨航次" in candidate.review_recommendation or "航次" in candidate.review_recommendation


def test_known_different_containers_never_become_candidate():
    left = make_event("k-a", container="C1")
    right = make_event(
        "k-b",
        container="C2",
        event_time_utc=shift(BASE, 5),
    )
    assert find_node_candidates([left, right], time_window=WINDOW) == []


def test_missing_container_bridge_does_not_transitively_merge():
    # 三个不同真实节点（箱号不同），缺箱号行分别与两侧时间接近。
    c1 = make_event("c1", container="C1", event_time_utc=BASE)
    bridge = make_event("bridge", container=None, event_time_utc=shift(BASE, 10))
    c2 = make_event("c2", container="C2", event_time_utc=shift(BASE, 20))
    candidates = find_node_candidates([c1, bridge, c2], time_window=WINDOW)

    assert all(candidate.shape == "pair" for candidate in candidates)
    assert all(candidate.linkage == "weak" for candidate in candidates)
    keys = {candidate.event_keys for candidate in candidates}
    assert keys == {("bridge", "c1"), ("bridge", "c2")}
    # 缺箱号不得把两个真实节点传递连接。
    assert not any(
        set(candidate.event_keys) == {"c1", "c2"} for candidate in candidates
    )
    for candidate in candidates:
        assert "缺箱" in candidate.review_recommendation


def test_adjacent_same_type_chain_emits_pairs_not_certain_group():
    chain = [
        make_event(
            f"node-{index}",
            container=None,
            event_time_utc=shift(BASE, index * 25),
        )
        for index in range(4)
    ]
    candidates = find_node_candidates(chain, time_window=WINDOW)

    assert all(candidate.shape == "pair" for candidate in candidates)
    # 相邻窗口对（间距 <=60 分钟）：0-1(25)、0-2(50)、1-2(25)、1-3(50)、2-3(25)；
    # 0-3 相距 75 分钟，超出窗口，不得通过传递窗口连成结论。
    emitted = {candidate.event_keys for candidate in candidates}
    assert ("node-0", "node-3") not in emitted
    assert ("node-0", "node-1") in emitted
    assert len(candidates) == 5
    assert all(
        any(reason.code == "container_missing_both" for reason in candidate.reasons)
        for candidate in candidates
    )


def test_strong_clique_is_group_and_weak_edges_stay_separate_pairs():
    # 三个箱号相同、两两时间接近的强边行 → clique 组。
    strong_rows = [
        make_event("g1", event_time_utc=BASE),
        make_event("g2", event_time_utc=shift(BASE, 10)),
        make_event("g3", event_time_utc=shift(BASE, 20)),
    ]
    # 缺箱号行只与 g1 弱关联（与 g2/g3 超出窗口）→ 独立 pair，不进组。
    weak_row = make_event(
        "weak", container=None, event_time_utc=shift(BASE, -55)
    )
    candidates = find_node_candidates(strong_rows + [weak_row], time_window=WINDOW)

    groups = [candidate for candidate in candidates if candidate.shape == "group"]
    pairs = [candidate for candidate in candidates if candidate.shape == "pair"]
    assert len(groups) == 1
    assert groups[0].event_keys == ("g1", "g2", "g3")
    assert groups[0].linkage == "strong"
    assert any(
        reason.code == "direct_pairwise_evidence" for reason in groups[0].reasons
    )
    # 组内 pair 不再重复输出。
    for pair in (("g1", "g2"), ("g1", "g3"), ("g2", "g3")):
        assert not any(
            set(candidate.event_keys) == set(pair) for candidate in pairs
        )
    weak_pairs = {candidate.event_keys for candidate in pairs}
    assert weak_pairs == {("g1", "weak")}


def test_strong_chain_without_clique_collapses_to_pairs():
    # g1-g2、g2-g3 强边，但 g1-g3 超出窗口：非 clique，禁止传递成组。
    rows = [
        make_event("g1", event_time_utc=BASE),
        make_event("g2", event_time_utc=shift(BASE, 20)),
        make_event("g3", event_time_utc=shift(BASE, 80)),
    ]
    candidates = find_node_candidates(rows, time_window=WINDOW)
    assert all(candidate.shape == "pair" for candidate in candidates)
    assert {candidate.event_keys for candidate in candidates} == {
        ("g1", "g2"),
        ("g2", "g3"),
    }


def test_port_and_event_type_mismatch_excluded():
    different_port = make_event(
        "other-port", port="CNNGB", event_time_utc=shift(BASE, 5)
    )
    different_type = make_event(
        "other-type", event_type="departed", event_time_utc=shift(BASE, 5)
    )
    base = make_event("base", event_time_utc=BASE)
    assert find_node_candidates([base, different_port], time_window=WINDOW) == []
    assert find_node_candidates([base, different_type], time_window=WINDOW) == []


def test_status_difference_is_evidence_only_and_does_not_adjudicate():
    left = make_event("k-a", status="estimated")
    right = make_event(
        "k-b", status="actual", event_time_utc=shift(BASE, 10)
    )
    candidates = find_node_candidates([left, right], time_window=WINDOW)

    candidate = candidates[0]
    conflict = next(
        conflict
        for conflict in candidate.conflicts
        if conflict.field == "status"
    )
    assert conflict.values == ("actual", "estimated")
    assert "人工" in conflict.detail
    # 不选择最终状态：两个成员各自保留原始 status。
    assert {member.status for member in candidate.members} == {
        "actual",
        "estimated",
    }
    assert "状态" in candidate.review_recommendation


def test_duplicate_source_refs_reported_once():
    left = make_event("k-a", sources=("dup-ref", "own-a"))
    right = make_event(
        "k-b",
        sources=("dup-ref", "own-b"),
        event_time_utc=shift(BASE, 10),
    )
    candidates = find_node_candidates([left, right], time_window=WINDOW)
    candidate = candidates[0]
    assert candidate.source_refs == ("dup-ref", "own-a", "own-b")
    assert any(
        reason.code == "shared_source_ref"
        and reason.detail == "dup-ref"
        for reason in candidate.reasons
    )


def test_each_unordered_pair_appears_at_most_once():
    rows = [
        make_event(
            f"row-{index}",
            container=None,
            event_time_utc=shift(BASE, index * 15),
        )
        for index in range(5)
    ]
    candidates = find_node_candidates(rows, time_window=WINDOW)
    emitted = all_pairs(candidates)
    assert len(emitted) == len(set(emitted))


def test_function_does_not_mutate_input():
    left = make_event("k-a")
    right = make_event("k-b", event_time_utc=shift(BASE, 10))
    original_sources = left.sources
    find_node_candidates([left, right], time_window=WINDOW)
    assert left.sources == original_sources
    assert left.event_key == "k-a"


def test_duplicate_event_key_is_rejected():
    with pytest.raises(ValueError):
        find_node_candidates(
            [
                make_event("same", event_time_utc=BASE),
                make_event("same", event_time_utc=shift(BASE, 10)),
            ],
            time_window=WINDOW,
        )


def test_readonly_loader_roundtrip_and_candidate_integration(tmp_path):
    database = tmp_path / "events.sqlite3"
    store = EventStore(database)
    try:
        store.upsert_many(
            [
                make_event("k-a", event_time_utc=BASE),
                make_event("k-b", event_time_utc=shift(BASE, 10)),
            ]
        )
    finally:
        store.close()

    loaded = load_shipment_events_readonly(database)
    assert {event.event_key for event in loaded} == {"k-a", "k-b"}
    assert loaded[0].sources == ("s1",)
    candidates = find_node_candidates(loaded, time_window=WINDOW)
    assert len(candidates) == 1
    assert candidates[0].event_keys == ("k-a", "k-b")


def test_readonly_loader_empty_database_and_missing_file(tmp_path):
    empty_database = tmp_path / "empty.sqlite3"
    sqlite3.connect(empty_database).close()
    assert load_shipment_events_readonly(empty_database) == []
    assert find_node_candidates(
        load_shipment_events_readonly(empty_database), time_window=WINDOW
    ) == []

    missing = tmp_path / "does-not-exist.sqlite3"
    assert load_shipment_events_readonly(missing) == []


def test_readonly_loader_never_writes(tmp_path):
    database = tmp_path / "events.sqlite3"
    store = EventStore(database)
    try:
        store.upsert_many([make_event("k-a", event_time_utc=BASE)])
    finally:
        store.close()

    snapshot = database.read_bytes()
    events = load_shipment_events_readonly(database)
    find_node_candidates(events, time_window=WINDOW)
    assert database.read_bytes() == snapshot

    # 只读连接上写操作必须失败。
    uri = database.resolve().as_uri() + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    try:
        with pytest.raises(sqlite3.OperationalError):
            connection.execute(
                "INSERT INTO shipment_events VALUES "
                "('hack','v','vy',NULL,'p','arrived','t','actual','s','o')"
            )
    finally:
        connection.close()


def test_database_row_order_does_not_affect_candidates(tmp_path):
    database = tmp_path / "events.sqlite3"
    store = EventStore(database)
    rows = [
        make_event("k-a", event_time_utc=BASE, sources=("a",)),
        make_event("k-b", event_time_utc=shift(BASE, 10), sources=("b",)),
        make_event(
            "k-c", container=None, event_time_utc=shift(BASE, 20), sources=("c",)
        ),
    ]
    try:
        store.upsert_many(rows)
    finally:
        store.close()

    first = find_node_candidates(
        load_shipment_events_readonly(database), time_window=WINDOW
    )
    # 以相反行号顺序重新读入（用独立连接反向 SELECT 模拟不同数据库顺序）。
    connection = sqlite3.connect(database)
    try:
        raw = connection.execute(
            "SELECT event_key, vessel, voyage, container, port, event_type, "
            "event_time_utc, status, sources, observed_at "
            "FROM shipment_events ORDER BY rowid DESC"
        ).fetchall()
    finally:
        connection.close()
    reversed_events = [
        ShipmentEvent(
            event_key=row[0],
            vessel=row[1],
            voyage=row[2],
            container=row[3],
            port=row[4],
            event_type=row[5],
            event_time_utc=row[6],
            status=row[7],
            sources=tuple(row[8].split(",")) if row[8] else (),
            observed_at=row[9],
        )
        for row in raw
    ]
    second = find_node_candidates(reversed_events, time_window=WINDOW)
    assert [candidate.candidate_id for candidate in first] == [
        candidate.candidate_id for candidate in second
    ]
    assert [candidate.event_keys for candidate in first] == [
        candidate.event_keys for candidate in second
    ]


def test_no_merging_or_status_override_across_candidates():
    rows = [
        make_event("k-a", status="estimated", sources=("s1",)),
        make_event(
            "k-b",
            status="cancelled",
            event_time_utc=shift(BASE, 10),
            sources=("s2",),
        ),
    ]
    candidates = find_node_candidates(rows, time_window=WINDOW)
    candidate = candidates[0]
    # 候选保留两行独立身份，event_key 不丢失、不合并。
    assert len(candidate.members) == 2
    assert {member.event_key for member in candidate.members} == {"k-a", "k-b"}
    assert "禁止自动合并" in candidate.review_recommendation


def test_single_row_produces_no_candidates():
    assert find_node_candidates([make_event("only")], time_window=WINDOW) == []


def test_reason_includes_all_five_judgement_dimensions():
    left = make_event("k-a")
    right = make_event("k-b", event_time_utc=shift(BASE, 10))
    candidates = find_node_candidates([left, right], time_window=WINDOW)
    codes = {reason.code for reason in candidates[0].reasons}
    # 航次、港口、事件类型、时间距离、箱号（已知一致）。
    assert {"same_voyage", "same_port", "same_event_type",
            "time_within_window", "container_match_known"} <= codes
    assert candidates[0].linkage == "strong"
    assert candidates[0].shape == "pair"


def test_vessel_difference_makes_weak_pair_with_conflict():
    left = make_event("k-a", vessel="VESSEL-1")
    right = make_event(
        "k-b",
        vessel="VESSEL-2",
        event_time_utc=shift(BASE, 10),
    )
    candidates = find_node_candidates([left, right], time_window=WINDOW)
    candidate = candidates[0]
    assert candidate.linkage == "weak"
    assert {conflict.field for conflict in candidate.conflicts} == {
        "vessel",
        "event_time_utc",
    }
    assert "船舶" in candidate.review_recommendation
