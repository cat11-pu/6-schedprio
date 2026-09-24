"""schedprio.py：调度内核（基线：先进先出，没有优先级与继承）。"""
from __future__ import annotations


class Scheduler:
    def __init__(self, quantum: int = 2):
        self.quantum = quantum
        self.tick = 0
        self.switches = 0
        self.trace = []
        self.finished = []

    def admit(self, task: dict) -> None:
        self.trace.append((0, task["id"], "admit"))

    def run(self, tasks, quanta: int = 20) -> list:
        """基线：按到达顺序轮转，忽略 task 里的 priority 字段。"""
        queue = list(tasks)
        order = []
        for _ in range(quanta * max(1, len(queue))):
            if not queue:
                break
            task = queue.pop(0)
            order.append(task["id"])
            self.tick += self.quantum
            self.switches += 1
            if task.get("work", 1) > 1:
                task = dict(task)
                task["work"] = task["work"] - 1
                queue.append(task)
            else:
                self.finished.append(task["id"])
        return order
