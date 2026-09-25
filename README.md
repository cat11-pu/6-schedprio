# schedprio

纯 Python 标准库的优先级调度内核，无第三方依赖。旧的 `Scheduler.admit()`、`Scheduler.run()` 和 `Lock.acquire/release/snapshot()` 兼容原签名；新增 `Scheduler.schedule()` 提供优先级、优先级继承和饥饿避免。

## 调度接口

```python
scheduler = Scheduler(quantum=2, starvation_threshold=10, max_priority=10)
plan = scheduler.schedule(tasks, locks, quanta)
```

任务字段：

- `id`：任务 ID；`priority` 数值越大越优先，默认 `0`。
- `work`：需要执行的 tick 数；`arrival`：到达 tick，默认 `0`。
- `needs`：初始化时等待的锁；`holds`：任务自身初始持有的锁。

锁配置支持 `{锁名: 初始持有者}` 或 `{锁名: [初始持有者]}`；列表后续元素登记为等待者。锁等待者按“有效优先级降序、到达顺序升序”排队。

返回字段：

- `order`：每个执行时间片的任务序列；`finished`：完成顺序。
- `switches`：执行时间片/切换次数；`inherit`：优先级继承触发次数。
- `starved`：等待超过阈值的任务；`boost_sequence` 与 `boosts`：老化提权序列。
- `inversion_ticks`：更高优先级任务等待较低基优先级持锁者执行的 tick 数。
- `invariant_ok`：调度不变量；`snapshot` 给出最终 tick、有效优先级和锁状态。

调度器只保存任务、就绪队列和每任务一个老化事件，不保存逐 tick 轨迹。

## 测试

    python3 -m unittest discover -s tests -v

## 场景自检

    python3 check_sample.py

`sample/tasks.json` 的验收结果：执行顺序为 `['high', 'mid', 'filler', 'filler', 'filler2', 'filler2']`，继承生效 1 次，饥饿任务为 `['low']`，切换 6 次，调度事件预算 60，任务数 5。
