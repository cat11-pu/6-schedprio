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


if __name__ == "__main__":
    unittest.main()
