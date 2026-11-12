# Port Events

港口船期与集装箱节点事件采集的独立实现。连接器、事件规范化、跨来源关联和持久化相互隔离；默认使用本地 JSON fixture，便于离线验收。

```bash
PYTHONPATH=src python3 -m port_events.cli --fixture fixtures/events.json --database /tmp/port-events.sqlite3
```

## 离线回放核对

上线前可用独立回放将 fixture 的假设结果与 SQLite 快照对照。回放复用 `JsonEventSource`、`normalize_event`、`merge_events` 算出事件集合，按完整 `event_key` 与全表只读快照比较，稳定输出两侧独有项及同键状态或来源集合变化；`observed_at` 波动不参与比较。快照以只读事务打开，路径缺失或结构不支持会明确失败且不会建库；fixture 解析或规范化失败返回含阶段与原因的不完整结果，剩余快照行不会被报为差异。回放绝不写库。

```bash
PYTHONPATH=src python3 -m port_events.replay --fixture fixtures/events.json --database /tmp/port-events.sqlite3
```

退出码：0 表示一致，1 表示存在差异，2 表示回放不完整（阶段与原因见 JSON 输出的 `stage` / `error`）。
