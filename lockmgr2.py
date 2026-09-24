"""lockmgr2.py：资源锁（基线：没有优先级继承）。"""
from __future__ import annotations


class Lock:
    def __init__(self, name: str):
        self.name = name
        self.owner = None
        self.waiters = []

    def acquire(self, task_id: str) -> bool:
        if self.owner is None:
            self.owner = task_id
            return True
        self.waiters.append(task_id)
        return False

    def release(self, task_id: str) -> bool:
        if self.owner != task_id:
            return False
        self.owner = self.waiters.pop(0) if self.waiters else None
        return True

    def snapshot(self) -> dict:
        return {"name": self.name, "owner": self.owner, "waiters": list(self.waiters)}
