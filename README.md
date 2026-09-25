# schedprio

纯 Python 标准库的 schedprio（无第三方依赖）。

## 调度语义

- `Scheduler.run(tasks)`：基线简单轮转（旧调用方兼容，忽略优先级）。
- `Scheduler.schedule(tasks, locks, quanta)`：按「有效优先级 + 到达序」轮转，
  每次派发消费一个 `quantum`，tick 预算为 `quanta`，返回
  `order / finished / switches / inherit / starved / inversion_ticks / invariant_ok`
  （另附 `boosts` 提权序列）。
- 优先级继承：高优先级任务等待低优先级持有者时，持有者临时提权到等待者的最高
  优先级，释放后恢复；`Scheduler.snapshot()` 给出各任务有效优先级。
- 老化：等待超过 `aging_threshold`（构造参数，默认 8 tick）提权一档，
  每任务最多 `max_aging_boosts` 档，避免高优先级被无限插队。
- 锁等待者按「有效优先级 + 到达序」排队；同一资源任一时刻只有一个持有者。
- 不保留逐 tick 轨迹，内存随任务数线性、随 tick 数常数；切换数不超过
  `ceil(quanta / quantum)`。

## 测试

    python3 -m unittest discover -s tests -v

## 场景自检

    python3 check_sample.py
