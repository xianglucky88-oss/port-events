"""只读近似节点候选识别。

以 shipment_events 规范化行（``ShipmentEvent``）作为唯一输入，识别 event_key
不同但可能指向同一物理节点、需要人工复核的行，稳定输出候选身份、完整键、
来源引用、相似依据、冲突字段与复核建议。

边界约定：

* 本模块只读：不采集外部数据、不做故障恢复、不分页、不写回任何存储，
  也绝不自动合并两行或裁定最终状态（状态差异仅作为人工证据）。
* 候选身份与排序只依赖行内容（event_key 及其字段），不依赖数据库顺序、
  临时行号或输入列表顺序。
* 缺箱号或时间滑窗形成的弱关联只能按对呈现，禁止通过传递关系把多个真实
  节点连成确定性的组结论。
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import combinations
from typing import Iterable

from .models import ShipmentEvent

DEFAULT_TIME_WINDOW = timedelta(hours=12)
_MISSING = ""


@dataclass(frozen=True)
class SimilarityReason:
    """单条相似依据。"""

    code: str
    detail: str


@dataclass(frozen=True)
class FieldConflict:
    """候选成员之间取值不一致的字段（仅证据，不做裁定）。"""

    field: str
    values: tuple[str, ...]
    detail: str


@dataclass(frozen=True)
class CandidateMember:
    """可回原行的候选成员快照，event_key 与 shipment_events 行一一对应。"""

    event_key: str
    vessel: str
    voyage: str
    container: str | None
    port: str
    event_type: str
    event_time_utc: str
    status: str
    sources: tuple[str, ...]


@dataclass(frozen=True)
class NodeCandidate:
    """一个近似节点候选，按对（pair）或按组（group）呈现。"""

    candidate_id: str
    shape: str  # "pair" | "group"
    linkage: str  # "strong" | "weak"
    event_keys: tuple[str, ...]
    members: tuple[CandidateMember, ...]
    source_refs: tuple[str, ...]
    reasons: tuple[SimilarityReason, ...]
    conflicts: tuple[FieldConflict, ...]
    review_recommendation: str


@dataclass(frozen=True)
class _Edge:
    left: str
    right: str
    strong: bool
    distance_seconds: int


def find_node_candidates(
    events: Iterable[ShipmentEvent],
    *,
    time_window: timedelta = DEFAULT_TIME_WINDOW,
) -> list[NodeCandidate]:
    """从规范化行中识别近似节点候选。

    纯函数：不执行任何 I/O，不修改输入，不合并、不写回。

    判定（航次、港口、事件类型、时间距离、container 已知/缺失共同参与）：

    * 港口与事件类型必须一致，时间距离不超过 ``time_window``（边界含等于）。
    * 双方箱号均已知且不同：确定为不同节点，不产生候选。
    * 强边：双方箱号均已知且相同，且航次、船舶一致。
    * 弱边：任一方缺箱号，或航次/船舶不一致，只能按对呈现。
    * 仅强边连通且成员两两直接满足条件（clique）的 3 行及以上集合才按组
      呈现；其余全部按对呈现，阻断缺箱号与连续窗口的传递性推断。
    """
    rows = sorted(events, key=lambda item: item.event_key)
    by_key = {item.event_key: item for item in rows}
    if len(by_key) != len(rows):
        raise ValueError("shipment_events 规范化行必须具有唯一 event_key")

    edges: list[_Edge] = []
    for index, left in enumerate(rows):
        for right in rows[index + 1 :]:
            edge = _evaluate_edge(left, right, time_window)
            if edge is not None:
                edges.append(edge)

    # 并查集仅用于聚合强边；弱边不参与任何连通性推断。
    parent = {key: key for key in by_key}

    def find_root(key: str) -> str:
        while parent[key] != key:
            parent[key] = parent[parent[key]]
            key = parent[key]
        return key

    strong_pair_set: set[tuple[str, str]] = set()
    for edge in edges:
        if edge.strong:
            strong_pair_set.add((edge.left, edge.right))
            root_left = find_root(edge.left)
            root_right = find_root(edge.right)
            if root_left != root_right:
                parent[root_right] = root_left

    strong_members: set[str] = set()
    for edge in edges:
        if edge.strong:
            strong_members.add(edge.left)
            strong_members.add(edge.right)

    components: dict[str, list[str]] = {}
    for key in strong_members:
        components.setdefault(find_root(key), []).append(key)

    candidates: list[NodeCandidate] = []
    emitted_pairs: set[tuple[str, str]] = set()
    grouped_roots: set[str] = set()

    # 强边连通分量：只有两两直接成立的 clique 才允许按组呈现。
    for root, member_keys in components.items():
        member_keys = sorted(member_keys)
        if len(member_keys) >= 3 and _is_clique(member_keys, strong_pair_set):
            grouped_roots.add(root)
            candidates.append(_build_group(member_keys, by_key, time_window))
            for pair in combinations(member_keys, 2):
                emitted_pairs.add(pair)

    # 其余边一律按对呈现：非 clique 强链不传递，弱边（缺箱号/跨航次）独立呈现。
    for edge in edges:
        pair = (edge.left, edge.right)
        if pair in emitted_pairs:
            continue
        if edge.strong and find_root(edge.left) in grouped_roots:
            continue
        candidates.append(_build_pair_candidate(edge, by_key, time_window))
        emitted_pairs.add(pair)

    # 内容排序：与输入顺序、数据库行号无关。
    candidates.sort(key=lambda candidate: candidate.event_keys)
    return candidates


def _evaluate_edge(
    left: ShipmentEvent, right: ShipmentEvent, time_window: timedelta
) -> _Edge | None:
    if left.event_key == right.event_key:
        return None
    if left.port != right.port or left.event_type != right.event_type:
        return None

    left_time = datetime.fromisoformat(left.event_time_utc)
    right_time = datetime.fromisoformat(right.event_time_utc)
    distance_seconds = round(abs((left_time - right_time).total_seconds()))
    if distance_seconds > time_window.total_seconds():
        return None

    # 双方箱号均已知且不同：确定为不同真实节点，不得作为候选。
    if (
        left.container is not None
        and right.container is not None
        and left.container != right.container
    ):
        return None

    strong = (
        left.container is not None
        and right.container is not None
        and left.container == right.container
        and left.voyage == right.voyage
        and left.vessel == right.vessel
    )
    return _Edge(left.event_key, right.event_key, strong, distance_seconds)


def _is_clique(
    member_keys: list[str], pair_set: set[tuple[str, str]]
) -> bool:
    for pair in combinations(member_keys, 2):
        if pair not in pair_set:
            return False
    return True


def _to_member(event: ShipmentEvent) -> CandidateMember:
    return CandidateMember(
        event_key=event.event_key,
        vessel=event.vessel,
        voyage=event.voyage,
        container=event.container,
        port=event.port,
        event_type=event.event_type,
        event_time_utc=event.event_time_utc,
        status=event.status,
        sources=tuple(event.sources),
    )


def _candidate_id(event_keys: tuple[str, ...]) -> str:
    # 身份完全由成员完整键的内容派生，跨进程、跨插入序稳定。
    digest = hashlib.sha256(
        "\u001f".join(event_keys).encode("utf-8")
    ).hexdigest()[:12]
    return f"cand-{digest}"


def _build_pair_candidate(
    edge: _Edge,
    by_key: dict[str, ShipmentEvent],
    time_window: timedelta,
) -> NodeCandidate:
    members = sorted(
        (_to_member(by_key[edge.left]), _to_member(by_key[edge.right])),
        key=lambda member: member.event_key,
    )
    event_keys = tuple(member.event_key for member in members)
    conflicts = _build_conflicts(members)
    return NodeCandidate(
        candidate_id=_candidate_id(event_keys),
        shape="pair",
        linkage="strong" if edge.strong else "weak",
        event_keys=event_keys,
        members=tuple(members),
        source_refs=_collect_source_refs(members),
        reasons=_pair_reasons(members[0], members[1], edge, time_window),
        conflicts=conflicts,
        review_recommendation=_recommendation("pair", members, conflicts),
    )


def _build_group(
    member_keys: list[str],
    by_key: dict[str, ShipmentEvent],
    time_window: timedelta,
) -> NodeCandidate:
    members = sorted(
        (_to_member(by_key[key]) for key in member_keys),
        key=lambda member: member.event_key,
    )
    event_keys = tuple(member.event_key for member in members)
    conflicts = _build_conflicts(members)
    return NodeCandidate(
        candidate_id=_candidate_id(event_keys),
        shape="group",
        linkage="strong",
        event_keys=event_keys,
        members=tuple(members),
        source_refs=_collect_source_refs(members),
        reasons=_group_reasons(members, time_window),
        conflicts=conflicts,
        review_recommendation=_recommendation("group", members, conflicts),
    )


def _collect_source_refs(members: list[CandidateMember]) -> tuple[str, ...]:
    refs = {ref for member in members for ref in member.sources}
    return tuple(sorted(refs))


def _pair_reasons(
    left: CandidateMember,
    right: CandidateMember,
    edge: _Edge,
    time_window: timedelta,
) -> tuple[SimilarityReason, ...]:
    reasons = [
        SimilarityReason("same_port", f"port={left.port}"),
        SimilarityReason("same_event_type", f"event_type={left.event_type}"),
        SimilarityReason(
            "time_within_window",
            f"distance_seconds={edge.distance_seconds}; "
            f"window_seconds={int(time_window.total_seconds())}",
        ),
    ]
    if left.container is None and right.container is None:
        reasons.append(
            SimilarityReason(
                "container_missing_both", "两行均缺箱号，无箱号佐证"
            )
        )
    elif left.container is None or right.container is None:
        reasons.append(
            SimilarityReason(
                "container_missing_one_side",
                f"container="
                f"{left.container or '<缺失>'}/{right.container or '<缺失>'}"
                "；缺箱方无法提供箱号佐证",
            )
        )
    else:
        reasons.append(
            SimilarityReason(
                "container_match_known", f"container={left.container}"
            )
        )
    if left.voyage == right.voyage:
        reasons.append(
            SimilarityReason("same_voyage", f"voyage={left.voyage}")
        )
    else:
        reasons.append(
            SimilarityReason(
                "voyage_differs",
                f"voyage={left.voyage}/{right.voyage}",
            )
        )
    if left.vessel == right.vessel:
        reasons.append(
            SimilarityReason("same_vessel", f"vessel={left.vessel}")
        )
    else:
        reasons.append(
            SimilarityReason(
                "vessel_differs", f"vessel={left.vessel}/{right.vessel}"
            )
        )
    shared_sources = sorted(set(left.sources) & set(right.sources))
    if shared_sources:
        reasons.append(
            SimilarityReason(
                "shared_source_ref", ";".join(shared_sources)
            )
        )
    return tuple(reasons)


def _group_reasons(
    members: list[CandidateMember], time_window: timedelta
) -> tuple[SimilarityReason, ...]:
    max_distance = 0
    shared_sources: set[str] = set()
    for left, right in combinations(members, 2):
        left_time = datetime.fromisoformat(left.event_time_utc)
        right_time = datetime.fromisoformat(right.event_time_utc)
        max_distance = max(
            max_distance,
            round(abs((left_time - right_time).total_seconds())),
        )
        shared_sources.update(set(left.sources) & set(right.sources))

    reasons = [
        SimilarityReason(
            "direct_pairwise_evidence",
            f"组内 {len(members)} 行两两直接满足相似条件，无传递性推断",
        ),
        SimilarityReason("same_port", f"port={members[0].port}"),
        SimilarityReason(
            "same_event_type", f"event_type={members[0].event_type}"
        ),
        SimilarityReason(
            "time_within_window",
            f"max_distance_seconds={max_distance}; "
            f"window_seconds={int(time_window.total_seconds())}",
        ),
        SimilarityReason(
            "container_match_known", f"container={members[0].container}"
        ),
        SimilarityReason(
            "same_voyage", f"voyage={members[0].voyage}"
        ),
        SimilarityReason(
            "same_vessel", f"vessel={members[0].vessel}"
        ),
    ]
    if shared_sources:
        reasons.append(
            SimilarityReason(
                "shared_source_ref", ";".join(sorted(shared_sources))
            )
        )
    return tuple(reasons)


def _render_value(value: str | None) -> str:
    return _MISSING if value is None else value


def _build_conflicts(
    members: list[CandidateMember],
) -> tuple[FieldConflict, ...]:
    conflicts: list[FieldConflict] = []
    for field in (
        "vessel",
        "voyage",
        "status",
        "container",
        "event_time_utc",
    ):
        values = tuple(
            sorted({_render_value(getattr(member, field)) for member in members})
        )
        if len(values) <= 1:
            continue
        conflicts.append(
            FieldConflict(field, values, _conflict_detail(field, members))
        )
    return tuple(sorted(conflicts, key=lambda conflict: conflict.field))


def _conflict_detail(field: str, members: list[CandidateMember]) -> str:
    if field == "status":
        return "状态差异仅作为人工证据，系统不裁定最终状态。"
    if field == "container":
        return "存在箱号缺失；缺失箱号不得视为匹配，需补全后复核。"
    if field == "event_time_utc":
        times = [
            datetime.fromisoformat(member.event_time_utc) for member in members
        ]
        distance = round(
            (max(times) - min(times)).total_seconds()
        )
        return f"组内最大时间偏差 {distance} 秒，请核对原始上报时间与时区。"
    return f"字段 {field} 取值不一致，需人工核对。"


def _recommendation(
    shape: str,
    members: list[CandidateMember],
    conflicts: tuple[FieldConflict, ...],
) -> str:
    parts = ["近似节点候选，仅用于人工复核，禁止自动合并。"]
    if shape == "group":
        parts.append("组内任意两行均有直接相似依据，未使用传递性推断。")
    if any(member.container is None for member in members):
        parts.append(
            "存在箱号缺失行，缺箱不得作为同一节点的确定依据，请补全箱号后复核。"
        )
    conflict_fields = {conflict.field for conflict in conflicts}
    if "voyage" in conflict_fields:
        parts.append("航次不一致，需确认是否属于不同航次的独立节点。")
    if "vessel" in conflict_fields:
        parts.append("船舶标识不一致，需核对船舶信息。")
    if "event_time_utc" in conflict_fields:
        parts.append("事件时间存在偏差，请核对原始上报时间与时区。")
    if "status" in conflict_fields:
        parts.append("状态差异仅作为人工证据，最终状态由人工裁定。")
    return "".join(parts)
