# Port Events

港口船期与集装箱节点事件采集的独立实现。连接器、事件规范化、跨来源关联和持久化相互隔离；默认使用本地 JSON fixture，便于离线验收。

## 导入（保持原有语义）

```bash
PYTHONPATH=src python3 -m port_events.cli --fixture fixtures/events.json --database /tmp/port-events.sqlite3
# 等价的显式子命令形式
PYTHONPATH=src python3 -m port_events.cli import --fixture fixtures/events.json --database /tmp/port-events.sqlite3
```

导入仍按 `event_key` 执行 `INSERT OR REPLACE`；每次写入在同一事务内将
`store_revision` 加一，供读取上下文标识自己看到的版本。

## 快照检索与键集分页

客服可按**箱号、船名、航次、港口、事件类型和 UTC 时间范围**（含端点）检索已提交事件，
逐页取回 `event_key`、`status` 与来源引用（`sources`）。

读取契约（`port_events.snapshot`）：

- **独立快照**：`open_snapshot()` 打开一个私有的只读 SQLite 连接并显式开启读事务
  （数据库使用 WAL）。快照在打开时固定读取版本，翻页期间发生的插入/覆盖对该快照不可见，
  因此不会漏行、重行，也不会伪造连续性。
- **过滤与排序全部下推 SQLite**：无 OFFSET、无整表载入，每页最多取 `page_size + 1` 行。
- **完整顺序续取**：以 `(event_time_utc, event_key)` 全序做键集分页，游标是不透明的
  签名令牌，绑定筛选指纹、快照 ID 与过期时间。
- **失效契约（带 reason）**：
  - `cursor_expired`：游标超过快照 TTL，需重开快照从首页重新分页；
  - `cursor_invalid`：令牌被篡改、属于其他快照/其他筛选条件（跨条件游标）；
  - `snapshot_closed`：快照已关闭；
  - `invalid_time_range` / `invalid_page_size` / `invalid_filter`：非法入参。

```python
from port_events.storage import EventStore
from port_events.snapshot import EventFilter

store = EventStore("/tmp/port-events.sqlite3")
event_filter = EventFilter(
    container="CONT001", vessel="Ocean Star", voyage="OS202611A",
    port="CNSHA", event_type="arrived",
    start_time_utc="2026-11-10T00:00:00Z",
    end_time_utc="2026-11-30T23:59:59Z",
)
with store.open_snapshot(event_filter, ttl_seconds=300) as snapshot:
    print(snapshot.revision)          # 快照绑定的 store_revision
    cursor = None
    while True:
        page = snapshot.fetch_page(cursor=cursor, page_size=50)
        for event in page.events:
            print(event.event_key, event.status, event.sources)
        if not page.has_more:
            break
        cursor = page.next_cursor
store.close()
```

船名/航次/港口按导入规则做大写归一；箱号与事件类型大小写敏感。船名级事件的
`container` 为 `NULL`，箱号过滤会通过 `container IS NOT NULL` 排除这些行。

## 可复现导出

```bash
PYTHONPATH=src python3 -m port_events.cli export \
  --database /tmp/port-events.sqlite3 --out /tmp/events.jsonl \
  --vessel "Ocean Star" --event-type arrived --page-size 100
```

导出为 JSON Lines：首行是元数据头（格式标识、快照 ID、revision、筛选条件、排序键、
`event_count`），其后每行一个事件，按 `(event_time_utc, event_key)` 排序。
分页经游标流式进行，事件行经磁盘 spool 中转，不将整表载入内存；输出文件以原子替换写入。
同一数据库版本、同一筛选条件重开快照导出可复现相同的有序结果。

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
# 安装 pytest 后同样兼容：
PYTHONPATH=src python3 -m pytest tests/
```

测试覆盖：同刻事件、船舶级无箱号、空结果、非法区间、跨条件游标、游标过期（含真实 TTL）、
翻页间写入（同进程与跨进程）、只读保护、SQL 下推与无 OFFSET、导出可复现性，以及导入语义不变。
范围止于筛选与分页，不包含更新/删除事件等写路径扩展。
