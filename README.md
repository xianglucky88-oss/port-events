# Port Events

港口船期与集装箱节点事件采集的独立实现。连接器、事件规范化、跨来源关联和持久化相互隔离；默认使用本地 JSON fixture，便于离线验收。

```bash
PYTHONPATH=src python3 -m port_events.cli --fixture fixtures/events.json --database /tmp/port-events.sqlite3
```

## 结构版本生命周期

`EventStore` 打开数据库时先完成独立的结构版本生命周期（`port_events.schema`），正常写入仅在其完成后开始：

- **全新文件**：初始化为当前十列结构（版本 2）并登记版本。
- **当前结构、无版本标记**（旧版本代码写入的文件）：收养现有数据，补登记版本。
- **声明支持的旧结构**（版本 1，九列）：按构造库中声明的字段映射无损升级——`source→sources`、`recorded_at→observed_at`、缺失的 `container` 置 NULL；不猜测时间、不重算键、不裁定状态。
- **未知结构或更高版本**：给出明确诊断（找到的列 / 版本号、支持的版本），文件保持原样。

升级采用恢复检查点：重建事务（新结构 + 逐行校验复制 + 隔离区）先提交，版本登记后提交。两步之间崩溃（DDL 完成但版本未标记）会在下次打开时仅补登记；重建中途崩溃则整体回滚后重跑。并发打开通过写锁串行化并重复检测，锁冲突给出明确诊断。无法无损映射的行（必填列为空、键重复、约束拒绝）进入 `shipment_events_quarantine`，保留原始内容与原因，其余行继续升级。每步前置、目标与版本登记写入 `schema_migrations` 供核对。
