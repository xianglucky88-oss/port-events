# Port Events

港口船期与集装箱节点事件采集的独立实现。连接器、事件规范化、跨来源关联和持久化相互隔离；默认使用本地 JSON fixture，便于离线验收。

```bash
PYTHONPATH=src python3 -m port_events.cli --fixture fixtures/events.json --database /tmp/port-events.sqlite3
```

## 多来源批次模式

`PortEventsPipeline.run_batches()` 一次接收多组 `SourceBatch(source_id, batch_id, source)`，各来源独立采集、互不影响：承运人文件暂不可读时，码头本轮事件照常提交；恢复后重跑同一批声明即可，只有未提交的批次会被重试。

```bash
PYTHONPATH=src python3 -m port_events.cli \
  --batch carrier:carrier-b1:carrier.json \
  --batch terminal:terminal-b1:terminal.json \
  --database /tmp/port-events.sqlite3
```

### 批次状态与恢复

批次日志持久化在 `event_batches` 表，状态机为：

- `pending`：已认领、结果未知（执行中或进程中断）；
- `failed`：上次尝试失败（含错误原因与尝试次数）；
- `committed`：已提交，终态。

重启后 `EventStore.retryable_batches()` 列出待执行批次（`pending` + `failed`）；`run_batches()` 对已提交批次直接跳过，只执行未完成批次。恢复粒度止于批次，不建逐事件审计。

### 提交策略：来源原子提交

每个来源批次独立提交：**事件写入与批次确认在同一个 SQLite 事务内**，故障后二者必然对应——要么都持久化（日志为 `committed`），要么都回滚（日志保持 `pending`/`failed`）。因此不引入暂存发布；跨批的精确 `event_key` 在写入时与库内已有记录按 `merge_events` 既有规则合并（actual 优先、来源取并集、observed_at 取最大）。

### 同批重放与新批次

`(source_id, batch_id)` 是投递标识：相同标识视为同批重放——已提交则跳过，未提交则重试（幂等，事件按 `event_key` 合并不重复）。来源内容变化必须搭配新的 `batch_id`，新批次按新投递处理并与既有记录合并。

### 重试条件

以下情况本批标记为可重试（`failed` 或保持 `pending`），其余来源继续：

- **读取失败**：`source.read()` 抛错（如承运人文件暂不可读）；
- **非法记录**：任一条记录规范化失败，整批拒绝、一条不写入；
- **锁冲突**：日志认领或提交时 SQLite `database is locked`，事务未生效；
- **提交不确定**：提交过程异常、结果未知——以批次日志为准，`committed` 跳过、否则重试，不会重复写入。

### 离线验证

`tests/test_batches.py` 用两个离线 JSON 来源覆盖：单方失败与恢复后只重试失败批次、整批失败且其他来源继续、同批重放跳过与新批次跨批合并、批次与记录顺序变化下最终状态一致、提交前/中/后崩溃重启的恢复，以及锁冲突重试。

```bash
PYTHONPATH=src python3 -m pytest tests/
```
