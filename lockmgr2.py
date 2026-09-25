"""lockmgr2.py：支持按有效优先级排队的资源锁。"""
from __future__ import annotations

from typing import Callable


class Lock:
    def __init__(self, name: str):
        self.name = name
        self.owner = None
        self.waiters = []
        self._arrival = {}
        self._counter = 0
        self.priority_provider: Callable[[str], int] | None = None
        self.inherited_priority_provider: Callable[[str, "Lock"], int] | None = None
        self.arrival_order: dict[str, int] | None = None

    def attach_priority_context(
        self,
        priority_provider: Callable[[str], int],
        arrival_order: dict[str, int] | None = None,
        inherited_priority_provider: Callable[[str, "Lock"], int] | None = None,
    ) -> None:
        self.priority_provider = priority_provider
        self.arrival_order = arrival_order
        self.inherited_priority_provider = inherited_priority_provider

    def acquire(self, task_id: str) -> bool:
        if self.owner is None:
            self.owner = task_id
            return True
        if task_id not in self._arrival:
            self._arrival[task_id] = self._counter
            self._counter += 1
        self.waiters.append(task_id)
        self._reorder_waiters()
        return False

    def release(self, task_id: str) -> bool:
        if self.owner != task_id:
            return False
        self.owner = None
        if self.waiters:
            self.waiters.sort(key=self._waiter_key)
            self.owner = self.waiters.pop(0)
        return True

    def reorder_waiters(self) -> None:
        self._reorder_waiters()

    def snapshot(self) -> dict:
        result = {"name": self.name, "owner": self.owner, "waiters": list(self.waiters)}
        if self.priority_provider is not None:
            if self.inherited_priority_provider is not None and self.owner is not None:
                owner_priority = self.inherited_priority_provider(self.owner, self)
            elif self.owner is not None:
                owner_priority = self.priority_provider(self.owner)
            else:
                owner_priority = None
            result["owner_effective_priority"] = owner_priority
            result["waiter_effective_priorities"] = {
                task_id: self.priority_provider(task_id) for task_id in self.waiters
            }
        return result

    def _reorder_waiters(self) -> None:
        if self.priority_provider is not None:
            self.waiters.sort(key=self._waiter_key)

    def _waiter_key(self, task_id: str) -> tuple[int, int]:
        priority = self.priority_provider(task_id) if self.priority_provider else 0
        if self.arrival_order is not None and task_id in self.arrival_order:
            arrival = self.arrival_order[task_id]
        else:
            arrival = self._arrival.get(task_id, self._counter)
        return (-priority, arrival)
