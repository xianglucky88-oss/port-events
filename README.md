# Port Events

港口船期与集装箱节点事件采集的独立实现。连接器、事件规范化、跨来源关联和持久化相互隔离；默认使用本地 JSON fixture，便于离线验收。

```bash
PYTHONPATH=src python3 -m port_events.cli --fixture fixtures/events.json --database /tmp/port-events.sqlite3
```

## 单作业序列质检（只读）

`port_events.quality` 对单船/航次/港口在左闭右开 UTC 窗口 `[start, end)` 内的
`actual` 行做序列质检，不推断重复靠港的作业归属，也不写库（以 `mode=ro` 打开 SQLite）：

- 船舶节点：`arrived <= berthed <= departed`；
- 每箱节点：`gate_in <= loaded`，且 `loaded` 不晚于全船唯一的 `departed`（同一 `departed` 可多箱共用）；
- 窗口内缺失角色 → `missing_evidence`；同一角色出现多行 → `role_conflict`（逐行附带 `event_key`）；
- 仅当比较双方角色都唯一时才判断时间顺序，违例 → `out_of_order`；
- 输出按 scope/container/类型/角色稳定排序的 `QualityIssue` 列表。

```python
from port_events.quality import check_database

issues = check_database(
    "/tmp/port-events.sqlite3",
    vessel="Ocean Star", voyage="OS202611A", port="CNSHA",
    window_start="2026-11-10T00:00:00+00:00",
    window_end="2026-11-13T00:00:00+00:00",
)
```
