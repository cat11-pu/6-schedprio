import unittest

from lockmgr2 import Lock
from schedprio import Scheduler


class TestScheduler(unittest.TestCase):
    def test_runs_all(self):
        scheduler = Scheduler()
        order = scheduler.run([{"id": "a"}, {"id": "b"}])
        self.assertEqual(sorted(set(order)), ["a", "b"])

    def test_work_multi_quanta(self):
        scheduler = Scheduler()
        order = scheduler.run([{"id": "a", "work": 2}])
        self.assertEqual(order.count("a"), 2)

    def test_finished_list(self):
        scheduler = Scheduler()
        scheduler.run([{"id": "a"}])
        self.assertIn("a", scheduler.finished)

    def test_lock_acquire_release(self):
        lock = Lock("db")
        self.assertTrue(lock.acquire("a"))
        self.assertTrue(lock.release("a"))
        self.assertIsNone(lock.snapshot()["owner"])

    def test_lock_waiter_queued(self):
        lock = Lock("db")
        lock.acquire("a")
        lock.acquire("b")
        self.assertEqual(lock.snapshot()["waiters"], ["b"])

    def test_priority_and_same_arrival_order(self):
        scheduler = Scheduler(quantum=2)
        plan = scheduler.schedule(
            [
                {"id": "low", "priority": 1, "work": 1},
                {"id": "high", "priority": 9, "work": 2},
                {"id": "mid", "priority": 5, "work": 2},
            ],
            None,
            10,
        )
        self.assertEqual(plan["order"], ["high", "mid", "low"])
        self.assertEqual(plan["finished"], ["high", "mid", "low"])
        self.assertTrue(plan["invariant_ok"])

    def test_lock_waiters_use_effective_priority_then_arrival(self):
        lock = Lock("db")
        lock.attach_priority_context(
            lambda task_id: {"low": 1, "mid": 5, "high": 9}[task_id],
            {"low": 0, "mid": 1, "high": 2},
        )
        lock.acquire("low")
        lock.acquire("mid")
        lock.acquire("high")
        self.assertEqual(lock.snapshot()["waiters"], ["high", "mid"])
        lock.release("low")
        self.assertEqual(lock.snapshot()["owner"], "high")

    def test_priority_inheritance_and_release_restore(self):
        scheduler = Scheduler(quantum=2)
        plan = scheduler.schedule(
            [
                {"id": "low", "priority": 1, "work": 4, "holds": ["db"]},
                {"id": "high", "priority": 9, "work": 2, "needs": ["db"]},
                {"id": "mid", "priority": 5, "work": 2},
            ],
            {},
            10,
        )
        self.assertEqual(plan["order"], ["low", "low", "high", "mid"])
        self.assertEqual(plan["inherit"], 1)
        self.assertEqual(plan["inversion_ticks"], 4)
        self.assertEqual(plan["snapshot"]["tasks"]["low"]["effective_priority"], 1)

    def test_aging_produces_boost_sequence(self):
        scheduler = Scheduler(quantum=2, starvation_threshold=2)
        plan = scheduler.schedule(
            [
                {"id": "high", "priority": 9, "work": 8},
                {"id": "low", "priority": 1, "work": 4},
            ],
            None,
            12,
        )
        self.assertEqual(
            plan["boost_sequence"],
            [
                {"tick": 4, "task": "low", "old_priority": 1, "new_priority": 2},
                {"tick": 8, "task": "low", "old_priority": 2, "new_priority": 3},
            ],
        )
        self.assertIn("low", plan["order"])
        self.assertTrue(plan["invariant_ok"])

    def test_schedule_switch_budget_at_scale(self):
        tasks = [
            {"id": f"task-{index}", "priority": index % 10, "work": 1}
            for index in range(10000)
        ]
        scheduler = Scheduler(quantum=1)
        plan = scheduler.schedule(tasks, None, 10000)
        self.assertEqual(len(plan["finished"]), 10000)
        self.assertEqual(plan["switches"], 10000)
        self.assertLessEqual(plan["switches"], 10000)


if __name__ == "__main__":
    unittest.main()
