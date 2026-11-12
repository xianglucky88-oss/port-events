# Port Events

港口船期与集装箱节点事件采集的独立实现。连接器、事件规范化、跨来源关联和持久化相互隔离；默认使用本地 JSON fixture，便于离线验收。

```bash
PYTHONPATH=src python3 -m port_events.cli --fixture fixtures/events.json --database /tmp/port-events.sqlite3
```

## 时间解释契约

`RawEvent` 到 `ShipmentEvent` 的时间解释不再依赖任何机器本地时区。

- 带显式偏移（`+08:00`、`Z`）的时间保留自身含义，忽略所有来源声明。
- 无偏移的本地时间按以下优先级取时区，且不会使用机器默认值：
  1. 记录自身的 `timezone` 字段（`record_declaration`）；
  2. 连接器声明（`JsonEventSource(source_timezones=...)`，`source_declaration`）；
  3. 调用者按来源提供的映射（`PortEventsPipeline.run(source_timezones=...)`，`source_mapping`）；
  4. 旧版全局默认（`normalize_event(raw, local_timezone)` / `run(timezone_name)`，`global_default`）。
- 同一来源出现互相冲突的声明（记录与连接器、连接器与调用者等）会抛出
  `TimeZoneDeclarationConflict`，不会静默覆盖。
- 每个事件通过 `time_interpretation` 暴露 `applied_timezone`、`time_rule`、
  `time_origin` 与 `disambiguation`（同时以同名属性可用）。

夏令时（DST）：

- 不存在的本地时刻（春季前跳的空档）抛出 `NonexistentLocalTimeError`；无法从
  该字面值推导 UTC 或 `event_key`。
- 重复的本地时刻（秋季回拨的重叠）抛出 `AmbiguousLocalTimeError`，需要用
  记录的 `fold`（0 或 1）或时间字符串中的显式偏移来消歧。所选的发生次序会改变
  UTC 时间与 `event_key`。
- 非法时区抛出 `InvalidTimeZoneError`；非法 `fold` 抛出 `InvalidFoldError`。

旧版行为保持可用：JSON 字段不变、`container` 可缺省、`normalize_event` 的
原有调用形式与 `run(timezone_name)` 仍然有效；`event_key` 与存储表结构不变，
不反推历史本地时间、不改旧行、不建迁移。

```bash
PYTHONPATH=src python3 -m port_events.cli --fixture fixtures/events.json \
    --source-timezone carrier-api=Asia/Shanghai \
    --source-timezone terminal-api=UTC
```
