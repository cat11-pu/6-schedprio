import json
import os
import unittest

from lockmgr2 import Lock
from schedprio import Scheduler

SAMPLE = os.path.join(os.path.dirname(__file__), "..", "sample", "tasks.json")


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

    def test_schedule_sample_acceptance(self):
        with open(SAMPLE, encoding="utf-8") as handle:
            spec = json.load(handle)
        scheduler = Scheduler(spec["quantum"])
        plan = scheduler.schedule(spec["tasks"], spec["locks"], spec["quanta"])
        self.assertEqual(plan["order"], ["high", "mid", "filler", "filler", "filler2", "filler2"])
        self.assertEqual(plan["finished"], ["high", "mid", "filler", "filler2"])
        self.assertEqual(plan["inversion_ticks"], 0)
        self.assertEqual(plan["inherit"], 1)
        self.assertEqual(plan["starved"], ["low"])
        self.assertEqual(plan["switches"], 6)
        self.assertTrue(plan["invariant_ok"])

    def test_schedule_inherit_and_restore(self):
        scheduler = Scheduler(2, aging_threshold=100)
        plan = scheduler.schedule(
            [
                {"id": "low", "priority": 1, "work": 4, "holds": ["db"]},
                {"id": "high", "priority": 9, "work": 2, "needs": ["db"]},
            ],
            {"db": ["low"]},
            2,
        )
        self.assertEqual(plan["inherit"], 1)
        snap = scheduler.snapshot()
        self.assertEqual(snap["tasks"]["low"]["inherited"], 0)
        self.assertEqual(snap["tasks"]["low"]["effective"], 1)

    def test_schedule_aging_boost(self):
        scheduler = Scheduler(2, aging_threshold=2)
        plan = scheduler.schedule(
            [
                {"id": "hi", "priority": 5, "work": 6},
                {"id": "lo", "priority": 1, "work": 1},
            ],
            {},
            6,
        )
        self.assertTrue(any(entry[1] == "lo" for entry in plan["boosts"]))
        self.assertEqual(plan["starved"], ["lo"])

    def test_schedule_scale_budget(self):
        total = 10_000
        tasks = [{"id": "t%d" % i, "priority": i % 7, "work": 1} for i in range(total)]
        scheduler = Scheduler(2)
        plan = scheduler.schedule(tasks, {}, 10_000)
        self.assertLessEqual(plan["switches"], (10_000 + 1) // 2)
        self.assertEqual(len(plan["finished"]) + len(plan["starved"]), total)


if __name__ == "__main__":
    unittest.main()
