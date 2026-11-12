# Port Events

港口船期与集装箱节点事件采集的独立实现。连接器、事件规范化、跨来源关联和持久化相互隔离；默认使用本地 JSON fixture，便于离线验收。

```bash
PYTHONPATH=src python3 -m port_events.cli --fixture fixtures/events.json --database /tmp/port-events.sqlite3
```

## 可靠投递（reliable_delivery）

数仓下游事实库断连或发送退出后，已交付修订仍须按序同步的独立组件。它与事件库完全隔离：
只接收调用方提交的不可变信封（收件方、既有 `event_key`、从 1 连续的修订号、原始载荷字节），
**从不扫描事件库或比较快照来发现修订**。

- 按 `(recipient, event_key)` 保持顺序；可持久接收乱序信封，缺少前序即阻塞后继。
- 同一三元组重提返回原投递身份（UUID5 确定性身份，跨进程稳定）；同号字节不同永久拒绝。
- 载荷仅原样存储与重发，不解释 status、source、observed_at；不同修订即使字节相同也分别发送。
- 独立 SQLite 数据库持久接受与进展，**持久提交后才确认接受**。
- 可替换适配器（进程内 `RecordingAdapter` / 离线 HTTP `HttpDeliveryAdapter`），
  HTTP 接收器独立持久去重，区分确认（200）、可重试（408/425/429/5xx/网络错误）与永久拒绝（其他 4xx）。
- 丢回执或确认前崩溃以稳定身份至少一次重发；接管会 bump 围栏 epoch 并回收租约，
  旧请求与迟到回执无法推进，接收端拒绝修订倒退。
- 永久拒绝保存原因并阻塞序列；显式重试或放弃（abandon）后才放行，其他序列不受影响。

```bash
# 离线接收器（独立数据库）
PYTHONPATH=src python3 -m port_events.reliable_delivery.cli \
  --database /tmp/delivery.sqlite3 receive \
  --receiver-database /tmp/receiver.sqlite3 --port 18080
# 接受不可变信封（持久提交后才确认）
PYTHONPATH=src python3 -m port_events.reliable_delivery.cli \
  --database /tmp/delivery.sqlite3 accept \
  --recipient fact-db-1 --event-key EVT-1 --revision 1 --payload-file revision-1.bin
# 驱动可投递信封直到空闲（缺前序/退避/永久拒绝时自然停止）
PYTHONPATH=src python3 -m port_events.reliable_delivery.cli \
  --database /tmp/delivery.sqlite3 send --receiver-url http://127.0.0.1:18080/deliver
# 查询持久进展；拒绝处置
PYTHONPATH=src python3 -m port_events.reliable_delivery.cli \
  --database /tmp/delivery.sqlite3 progress --recipient fact-db-1 --event-key EVT-1
PYTHONPATH=src python3 -m port_events.reliable_delivery.cli \
  --database /tmp/delivery.sqlite3 retry-rejected --delivery-id <uuid>
PYTHONPATH=src python3 -m port_events.reliable_delivery.cli \
  --database /tmp/delivery.sqlite3 abandon --delivery-id <uuid> --reason "skip poison revision"
```
