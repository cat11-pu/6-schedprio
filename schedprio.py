"""schedprio.py：调度内核。

兼容保留：`run`/`admit` 维持基线的简单轮转行为。
新增：`schedule` 按「有效优先级 + 到达序」轮转，支持优先级继承与老化提权。
"""
from __future__ import annotations

import heapq

from lockmgr2 import Lock


class Scheduler:
    def __init__(self, quantum: int = 2, aging_threshold: int = 8, max_aging_boosts: int = 3):
        self.quantum = quantum
        self.aging_threshold = aging_threshold
        self.max_aging_boosts = max_aging_boosts
        self.tick = 0
        self.switches = 0
        self.trace = []
        self.finished = []
        self._tasks = {}
        self._locks = {}

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

    def schedule(self, tasks, locks, quanta: int) -> dict:
        """优先级调度主循环。

        每次派发消费一个 quantum（不足一个则取剩余预算），tick 预算为 quanta。
        只保留聚合计数与堆结构，不保留逐 tick 轨迹，内存随任务数线性、随 tick 数常数。
        """
        lock_objs = {}
        for name, holders in (locks or {}).items():
            if isinstance(holders, str):
                holders = [holders]
            lock = Lock(name)
            if holders:
                lock.owner = holders[0]
                lock.waiters.extend(holders[1:])
            lock_objs[name] = lock

        state = {}
        arrival_order = []
        for arrival, spec in enumerate(tasks):
            tid = spec["id"]
            base = spec.get("priority", 0)
            state[tid] = {
                "id": tid,
                "base": base,
                "aging": 0,
                "inherit": 0,
                "eff": base,
                "work": max(0, spec.get("work", 1)),
                "arrival": arrival,
                "needs": list(spec.get("needs", [])),
                "holds": list(spec.get("holds", [])),
                "version": 0,
                "ran": 0,
                "since": 0,
                "done": False,
            }
            arrival_order.append(tid)

        heap = [(-st["eff"], st["arrival"], st["version"], tid) for tid, st in state.items()]
        heapq.heapify(heap)
        aging_heap = [(self.aging_threshold + 1, tid) for tid in arrival_order]
        heapq.heapify(aging_heap)
        donations = {}
        displaced = {}

        def refresh(st):
            st["eff"] = max(st["base"] + st["aging"], st["inherit"])
            st["version"] += 1
            heapq.heappush(heap, (-st["eff"], st["arrival"], st["version"], st["id"]))

        def valid(st, version):
            return not st["done"] and st["version"] == version

        def peek_eff():
            while heap:
                neg_eff, _arr, ver, tid = heap[0]
                st = state[tid]
                if not valid(st, ver):
                    heapq.heappop(heap)
                    continue
                return -neg_eff
            return None

        def insert_waiter(lock, st):
            key = (-st["eff"], st["arrival"])
            pos = len(lock.waiters)
            for idx, other in enumerate(lock.waiters):
                ost = state.get(other)
                okey = (-ost["eff"], ost["arrival"]) if ost is not None else (0, 0)
                if key < okey:
                    pos = idx
                    break
            lock.waiters.insert(pos, st["id"])

        def release_lock(lname, owner_id):
            lock = lock_objs[lname]
            if lock.owner != owner_id:
                return
            for oid, _level in donations.pop(lname, []):
                ost = state.get(oid)
                if ost is None:
                    continue
                ost["inherit"] = 0
                for ds in donations.values():
                    for o, lvl in ds:
                        if o == oid:
                            ost["inherit"] = max(ost["inherit"], lvl)
                refresh(ost)
            if lock.waiters:
                lock.owner = lock.waiters.pop(0)
            elif lname in displaced:
                lock.owner = displaced.pop(lname)
            else:
                lock.owner = None

        order = []
        finished = []
        boosts = []
        inherit_count = 0
        inversion_ticks = 0
        switches = 0
        invariant_ok = True
        tick = 0

        while tick < quanta:
            while aging_heap and aging_heap[0][0] <= tick:
                _due, tid = heapq.heappop(aging_heap)
                st = state[tid]
                if st["done"]:
                    continue
                if tick - st["since"] > self.aging_threshold:
                    if st["aging"] < self.max_aging_boosts:
                        old = st["eff"]
                        st["aging"] += 1
                        refresh(st)
                        boosts.append((tick, tid, old, st["eff"]))
                        heapq.heappush(aging_heap, (tick + self.aging_threshold + 1, tid))
                else:
                    heapq.heappush(aging_heap, (st["since"] + self.aging_threshold + 1, tid))

            current = None
            while heap:
                _neg, _arr, ver, tid = heapq.heappop(heap)
                st = state[tid]
                if valid(st, ver):
                    current = st
                    break
            if current is None:
                break

            blocked = False
            for lname in current["needs"]:
                lock = lock_objs.get(lname)
                if lock is None:
                    continue
                if current["id"] in lock.waiters:
                    lock.waiters.remove(current["id"])
                if lock.owner in (None, current["id"]):
                    lock.owner = current["id"]
                    if lname not in current["holds"]:
                        current["holds"].append(lname)
                    continue
                owner = state.get(lock.owner)
                if owner is not None and owner["eff"] < current["eff"]:
                    if owner["inherit"] < current["eff"]:
                        owner["inherit"] = current["eff"]
                        refresh(owner)
                        inherit_count += 1
                    donations.setdefault(lname, []).append((owner["id"], current["eff"]))
                    displaced.setdefault(lname, lock.owner)
                    lock.owner = current["id"]
                    if lname not in current["holds"]:
                        current["holds"].append(lname)
                else:
                    insert_waiter(lock, current)
                    blocked = True
                    break
            if blocked:
                refresh(current)
                tick += 1
                continue

            top_eff = peek_eff()
            slice_len = min(self.quantum, quanta - tick)
            if top_eff is not None and top_eff > current["eff"]:
                inversion_ticks += slice_len
                invariant_ok = False

            tick += slice_len
            switches += 1
            order.append(current["id"])
            current["ran"] += 1
            current["work"] -= slice_len
            current["since"] = tick
            if current["work"] <= 0:
                current["done"] = True
                finished.append(current["id"])
                for lname in list(current["holds"]):
                    if lname in lock_objs:
                        release_lock(lname, current["id"])
                current["holds"] = []
            else:
                heapq.heappush(heap, (-current["eff"], current["arrival"], current["version"], current["id"]))

        starved = [tid for tid in arrival_order if state[tid]["ran"] == 0]

        self.tick += tick
        self.switches += switches
        self.finished.extend(finished)
        self._tasks = state
        self._locks = lock_objs
        return {
            "order": order,
            "finished": finished,
            "switches": switches,
            "inherit": inherit_count,
            "starved": starved,
            "inversion_ticks": inversion_ticks,
            "invariant_ok": invariant_ok,
            "boosts": boosts,
            "ticks": tick,
            "tasks": len(arrival_order),
        }

    def snapshot(self) -> dict:
        """各任务的有效优先级（基础 + 老化提权 + 继承提权）与锁状态。"""
        return {
            "tick": self.tick,
            "tasks": {
                tid: {
                    "base": st["base"],
                    "effective": st["eff"],
                    "aging": st["aging"],
                    "inherited": st["inherit"],
                    "remaining": max(0, st["work"]),
                    "done": st["done"],
                }
                for tid, st in self._tasks.items()
            },
            "locks": {name: lock.snapshot() for name, lock in self._locks.items()},
        }
