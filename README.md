# Port Events

港口船期与集装箱节点事件采集的独立实现。连接器、事件规范化、跨来源关联和持久化相互隔离；默认使用本地 JSON fixture，便于离线验收。

```bash
PYTHONPATH=src python3 -m port_events.cli --fixture fixtures/events.json --database /tmp/port-events.sqlite3
```

## 只读近似节点候选识别

两条 `event_key` 不同的已提交记录可能因时间接近或一方缺箱号而需要人工核对，
但系统**不得自动合并**。`port_events.candidates` 以 `shipment_events` 规范化行
作为唯一输入，纯函数、只读地输出候选身份、完整键、来源引用、相似依据、冲突
字段与复核建议：

```bash
PYTHONPATH=src python3 -m port_events.cli \
  --candidates-report --database /tmp/port-events.sqlite3 \
  --candidate-window-hours 12
```

判定与输出约束：

- 航次、港口、事件类型、时间距离（窗口含边界）与 container 已知/缺失共同判定；
  双方箱号均已知且不同即确定为不同节点，不产生候选。
- 仅“双方箱号已知且相同 + 航次/船舶一致”的强边可成组，且组内必须两两直接
  满足条件（clique）；缺箱号、跨航次等弱边只按对呈现，阻断传递性推断，
  防止缺箱号或连续窗口把多个真实节点连成确定结论。
- 候选身份由成员完整键的内容哈希派生，排序只依赖 `event_key` 内容，
  与数据库顺序、临时行号、插入序无关；每个候选可经 `event_keys` 回原行。
- 状态差异只作为冲突字段中的人工证据，不选择或覆盖最终状态。
- 只读路径以 SQLite `mode=ro` 打开，不采集、不恢复、不分页、不写回；
  空库与缺失文件返回空候选。
