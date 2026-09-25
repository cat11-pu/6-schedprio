"""把 sample/tasks.json 跑一遍，打印验收面（两个子系统）。"""
import json
import os
import sys

from lockmgr2 import Lock
from schedprio import Scheduler


def main() -> int:
    path = sys.argv[1] if len(sys.argv) > 1 else os.path.join("sample", "tasks.json")
    with open(path, encoding="utf-8") as handle:
        spec = json.load(handle)
    scheduler = Scheduler(spec["quantum"])
    plan = scheduler.schedule(spec["tasks"], spec["locks"], spec["quanta"])
    print("执行顺序 =", plan["order"])
    print("完成的顺序 =", plan["finished"])
    print("高优先级等待低优先级的总 tick =", plan["inversion_ticks"])
    print("继承生效次数 =", plan["inherit"])
    print("饥饿发生的任务 =", plan["starved"])
    print("上下文切换次数 =", plan["switches"])
    print("量级（调度事件数） =", len(spec["tasks"]) * spec["quanta"])
    print("不变量（高优先级不越级等待） =", plan["invariant_ok"])
    print("提权序列 =", plan["boosts"])
    print("任务数 =", len(spec["tasks"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
