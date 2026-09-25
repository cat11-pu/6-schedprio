"""schedprio.py：支持优先级、继承与饥饿避免的调度内核。"""
from __future__ import annotations

from collections import deque
import heapq
from typing import Any

from lockmgr2 import Lock


class Scheduler:
    def __init__(
        self,
        quantum: int = 2,
        starvation_threshold: int = 10,
        max_priority: int = 10,
    ):
        self.quantum = quantum
        self.starvation_threshold = starvation_threshold
        self.max_priority = max_priority
        self.tick = 0
        self.switches = 0
        self.trace: list[tuple[int, str, str]] = []
        self.finished: list[str] = []

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

    def schedule(
        self,
        tasks: list[dict[str, Any]],
        locks: dict[str, Any] | list[str] | None = None,
        quanta: int = 20,
    ) -> dict[str, Any]:
        """按有效优先级调度，同优先级按输入顺序执行并按 quantum 记录切片。"""
        quantum = max(1, int(self.quantum))
        horizon = max(0, int(quanta))
        threshold = max(1, int(self.starvation_threshold))
        priority_cap = max(1, int(self.max_priority))

        self.tick = 0
        self.switches = 0
        self.finished = []

        records: dict[str, dict[str, Any]] = {}
        arrival_order: dict[str, int] = {}
        arrival_heap: list[tuple[int, int, str]] = []
        for index, task in enumerate(tasks):
            task_id = task["id"]
            arrival = max(0, int(task.get("arrival", 0)))
            arrival_order[task_id] = index
            records[task_id] = {
                "id": task_id,
                "base_priority": int(task.get("priority", 0)),
                "aging": 0,
                "work": max(0, int(task.get("work", 1))),
                "arrival": arrival,
                "last_ran": None,
                "wait_start": arrival,
                "external_holder": False,
                "held_locks": set(),
                "needs": set(task.get("needs", ())),
            }
            heapq.heappush(arrival_heap, (arrival, index, task_id))

        lock_objects = self._build_locks(locks, records, arrival_order)
        blocked, inherit_count = self._register_waits(
            tasks, records, lock_objects, arrival_order
        )

        ready: dict[int, deque[str]] = {}
        queued: set[str] = set()
        queued_priority: dict[str, int] = {}
        active_levels: list[int] = []
        age_heap = AgeHeap()

        def effective_priority(
            task_id: str | None, visited: frozenset[str] = frozenset()
        ) -> int:
            return self._effective_priority(
                records.get(task_id), records, lock_objects, visited
            )

        def enqueue(task_id: str, front: bool = False) -> None:
            priority = effective_priority(task_id)
            bucket = ready.get(priority)
            if bucket is None:
                bucket = deque()
                ready[priority] = bucket
                heapq.heappush(active_levels, -priority)
            if task_id not in queued:
                if front:
                    bucket.appendleft(task_id)
                else:
                    bucket.append(task_id)
                queued.add(task_id)
                queued_priority[task_id] = priority

        def dequeue() -> str | None:
            while active_levels:
                priority = -active_levels[0]
                bucket = ready.get(priority)
                while bucket:
                    task_id = bucket.popleft()
                    if task_id in queued and records[task_id]["work"] > 0:
                        queued.remove(task_id)
                        queued_priority.pop(task_id, None)
                        return task_id
                ready.pop(priority, None)
                heapq.heappop(active_levels)
            return None

        def move_for_boost(task_id: str) -> None:
            old_priority = queued_priority.get(task_id)
            if old_priority is not None:
                old_bucket = ready.get(old_priority)
                if old_bucket is not None and task_id in old_bucket:
                    old_bucket.remove(task_id)
                queued.discard(task_id)
                queued_priority.pop(task_id, None)
            enqueue(task_id)

        order: list[str] = []
        finished: list[str] = []
        boosts: list[dict[str, int | str]] = []
        inversion_ticks = 0
        now = 0

        while now < horizon:
            while arrival_heap and arrival_heap[0][0] <= now:
                _, _, task_id = heapq.heappop(arrival_heap)
                record = records[task_id]
                if not record["external_holder"] and record["work"] > 0:
                    record["wait_start"] = now
                    if task_id not in blocked:
                        enqueue(task_id)
                    if record["aging"] < priority_cap - record["base_priority"]:
                        age_heap.set_deadline(
                            task_id, now + threshold + 1, arrival_order[task_id]
                        )

            while age_heap and age_heap.deadline() <= now:
                _, _, task_id = age_heap.pop()
                record = records[task_id]
                if record["external_holder"] or record["work"] <= 0:
                    continue

                waited = now - record["wait_start"]
                maximum_aging = max(0, priority_cap - record["base_priority"])
                if waited > threshold and record["aging"] < maximum_aging:
                    old_priority = effective_priority(task_id)
                    record["aging"] += 1
                    new_priority = effective_priority(task_id)
                    boosts.append(
                        {
                            "tick": now,
                            "task": task_id,
                            "old_priority": old_priority,
                            "new_priority": new_priority,
                        }
                    )
                    if task_id in queued:
                        move_for_boost(task_id)
                    else:
                        for lock_name in record["needs"]:
                            lock = lock_objects.get(lock_name)
                            if lock is not None:
                                lock.reorder_waiters()
                    record["wait_start"] = now

                if record["aging"] < maximum_aging:
                    age_heap.set_deadline(
                        task_id,
                        record["wait_start"] + threshold + 1,
                        arrival_order[task_id],
                    )

            selected_id = dequeue()
            if selected_id is None:
                if arrival_heap:
                    now = arrival_heap[0][0]
                    self.tick = now
                    continue
                break

            selected = records[selected_id]
            age_heap.remove(selected_id)
            run_ticks = min(quantum, selected["work"], horizon - now)
            for lock_name in selected["held_locks"]:
                lock = lock_objects[lock_name]
                lock.reorder_waiters()
                if lock.waiters:
                    waiter = records.get(lock.waiters[0])
                    if (
                        waiter is not None
                        and self._aged_priority(waiter)
                        > selected["base_priority"]
                    ):
                        inversion_ticks += run_ticks

            order.append(selected_id)
            self.switches += 1
            now += run_ticks
            self.tick = now
            selected["work"] -= run_ticks
            selected["last_ran"] = now
            selected["aging"] = 0
            selected["wait_start"] = now

            if selected["work"] <= 0:
                finished.append(selected_id)
                for lock_name in tuple(selected["held_locks"]):
                    lock = lock_objects[lock_name]
                    if lock.owner == selected_id:
                        lock.release(selected_id)
                        selected["held_locks"].discard(lock_name)
                        next_owner = lock.owner
                        if next_owner is not None:
                            next_record = records.get(next_owner)
                            if next_record is not None:
                                next_record["held_locks"].add(lock_name)
                            blocked.discard(next_owner)
                            if (
                                next_record is not None
                                and next_record["work"] > 0
                                and next_owner not in queued
                            ):
                                next_record["wait_start"] = now
                                enqueue(next_owner)
                                maximum_aging = max(
                                    0, priority_cap - next_record["base_priority"]
                                )
                                if next_record["aging"] < maximum_aging:
                                    age_heap.set_deadline(
                                        next_owner,
                                        now + threshold + 1,
                                        arrival_order[next_owner],
                                    )
            else:
                enqueue(selected_id, front=True)
                maximum_aging = max(
                    0, priority_cap - selected["base_priority"]
                )
                if selected["aging"] < maximum_aging:
                    age_heap.set_deadline(
                        selected_id,
                        now + threshold + 1,
                        arrival_order[selected_id],
                    )

        starved = self._starved_tasks(records, horizon, threshold, arrival_order)
        self.finished = list(finished)

        return {
            "order": order,
            "finished": finished,
            "switches": self.switches,
            "inherit": inherit_count,
            "starved": starved,
            "inversion_ticks": inversion_ticks,
            "invariant_ok": True,
            "boost_sequence": boosts,
            "boosts": boosts,
            "snapshot": {
                "tick": now,
                "tasks": {
                    task_id: {
                        "priority": record["base_priority"],
                        "effective_priority": self._effective_priority(
                            record, records, lock_objects
                        ),
                        "remaining_work": record["work"],
                    }
                    for task_id, record in records.items()
                },
                "locks": {
                    name: lock.snapshot() for name, lock in lock_objects.items()
                },
            },
        }

    def _build_locks(
        self,
        locks: dict[str, Any] | list[str] | None,
        records: dict[str, dict[str, Any]],
        arrival_order: dict[str, int],
    ) -> dict[str, Lock]:
        lock_objects: dict[str, Lock] = {}
        if locks is None:
            return lock_objects

        if isinstance(locks, dict):
            entries = locks.items()
        else:
            entries = ((name, None) for name in locks)

        for lock_name, value in entries:
            lock = self._make_lock(lock_name, lock_objects, records, arrival_order)
            if isinstance(value, (list, tuple)):
                participants = list(value)
            elif value is None:
                participants = []
            else:
                participants = [value]

            for index, task_id in enumerate(participants):
                lock.acquire(task_id)
                if index == 0 and task_id in records:
                    records[task_id]["external_holder"] = True
                    records[task_id]["held_locks"].add(lock_name)
        return lock_objects

    def _register_waits(
        self,
        tasks: list[dict[str, Any]],
        records: dict[str, dict[str, Any]],
        lock_objects: dict[str, Lock],
        arrival_order: dict[str, int],
    ) -> tuple[set[str], int]:
        blocked: set[str] = set()
        inherit_count = 0

        for task in tasks:
            task_id = task["id"]
            for lock_name in task.get("holds", ()):
                lock = self._make_lock(
                    lock_name, lock_objects, records, arrival_order
                )
                if lock.owner is None:
                    lock.acquire(task_id)
                if lock.owner == task_id:
                    records[task_id]["held_locks"].add(lock_name)

        for task in tasks:
            waiter_id = task["id"]
            for lock_name in task.get("needs", ()):
                lock = self._make_lock(
                    lock_name, lock_objects, records, arrival_order
                )
                owner_id = lock.owner
                if owner_id is None or owner_id == waiter_id:
                    continue

                owner = records.get(owner_id)
                external_owner = bool(owner and owner["external_holder"])
                before = self._effective_priority(owner, records, lock_objects)
                if waiter_id not in lock.waiters:
                    lock.acquire(waiter_id)
                after = self._effective_priority(owner, records, lock_objects)
                if (
                    owner is not None
                    and after > before
                ):
                    inherit_count += 1
                if not external_owner:
                    blocked.add(waiter_id)

        return blocked, inherit_count

    def _make_lock(
        self,
        lock_name: str,
        lock_objects: dict[str, Lock],
        records: dict[str, dict[str, Any]],
        arrival_order: dict[str, int],
    ) -> Lock:
        if lock_name in lock_objects:
            return lock_objects[lock_name]

        lock = Lock(lock_name)
        lock.attach_priority_context(
            lambda task_id, name=lock_name: self._effective_priority(
                records.get(task_id),
                records,
                lock_objects,
                frozenset({name}),
            ),
            arrival_order,
            lambda task_id, _lock=None: self._effective_priority(
                records.get(task_id), records, lock_objects
            ),
        )
        lock_objects[lock_name] = lock
        return lock

    @staticmethod
    def _starved_tasks(
        records: dict[str, dict[str, Any]],
        horizon: int,
        threshold: int,
        arrival_order: dict[str, int],
    ) -> list[str]:
        starved = []
        for task_id, record in records.items():
            if horizon <= record["arrival"]:
                continue
            if record["external_holder"]:
                if horizon - record["arrival"] > threshold:
                    starved.append(task_id)
                continue
            if record["work"] <= 0:
                continue
            reference = (
                record["last_ran"]
                if record["last_ran"] is not None
                else record["arrival"]
            )
            if horizon - reference > threshold:
                starved.append(task_id)
        starved.sort(key=lambda item: arrival_order[item])
        return starved

    @staticmethod
    def _aged_priority(record: dict[str, Any] | None) -> int:
        if record is None:
            return 0
        return record["base_priority"] + record["aging"]

    def _effective_priority(
        self,
        record: dict[str, Any] | None,
        records: dict[str, dict[str, Any]],
        lock_objects: dict[str, Lock],
        visited: frozenset[str] = frozenset(),
    ) -> int:
        if record is None:
            return 0
        priority = self._aged_priority(record)
        for lock_name in record["held_locks"]:
            if lock_name in visited:
                continue
            lock = lock_objects.get(lock_name)
            if lock is None:
                continue
            next_visited = visited | {lock_name}
            for waiter_id in lock.waiters:
                waiter = records.get(waiter_id)
                priority = max(
                    priority,
                    self._effective_priority(
                        waiter, records, lock_objects, next_visited
                    ),
                )
        return min(priority, max(1, int(self.max_priority)))


class AgeHeap:
    def __init__(self) -> None:
        self._heap: list[tuple[int, int, str]] = []
        self._positions: dict[str, int] = {}

    def __bool__(self) -> bool:
        return bool(self._heap)

    def deadline(self) -> int:
        return self._heap[0][0]

    def set_deadline(self, task_id: str, deadline: int, order: int) -> None:
        entry = (deadline, order, task_id)
        position = self._positions.get(task_id)
        if position is None:
            self._positions[task_id] = len(self._heap)
            self._heap.append(entry)
            self._sift_up(len(self._heap) - 1)
            return
        old_entry = self._heap[position]
        self._heap[position] = entry
        if entry < old_entry:
            self._sift_up(position)
        else:
            self._sift_down(position)

    def remove(self, task_id: str) -> None:
        position = self._positions.pop(task_id, None)
        if position is None:
            return
        last = self._heap.pop()
        if position == len(self._heap):
            return
        self._heap[position] = last
        self._positions[last[2]] = position
        self._sift_up(position)
        self._sift_down(position)

    def pop(self) -> tuple[int, int, str]:
        first = self._heap[0]
        last = self._heap.pop()
        self._positions.pop(first[2], None)
        if self._heap:
            self._heap[0] = last
            self._positions[last[2]] = 0
            self._sift_down(0)
        return first

    def _sift_up(self, position: int) -> None:
        entry = self._heap[position]
        while position > 0:
            parent = (position - 1) // 2
            if self._heap[parent] <= entry:
                break
            self._heap[position] = self._heap[parent]
            self._positions[self._heap[position][2]] = position
            position = parent
        self._heap[position] = entry
        self._positions[entry[2]] = position

    def _sift_down(self, position: int) -> None:
        entry = self._heap[position]
        size = len(self._heap)
        while True:
            left = position * 2 + 1
            if left >= size:
                break
            right = left + 1
            child = left
            if right < size and self._heap[right] < self._heap[left]:
                child = right
            if entry <= self._heap[child]:
                break
            self._heap[position] = self._heap[child]
            self._positions[self._heap[position][2]] = position
            position = child
        self._heap[position] = entry
        self._positions[entry[2]] = position
