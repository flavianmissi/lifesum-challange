import unittest
import multiprocessing
from challange.requests_pool import ActiveRequestsPool, worker
import time


MAX_REQUESTS_PER_SEC = 5
namespace = multiprocessing.Manager().Namespace()
namespace.worker_called_times = 0


def fake_worker(sem, pool):
    name = multiprocessing.current_process().name
    with sem:
        pool.activate(name)
        # should actually be doing some work
        namespace.worker_called_times += 1
        time.sleep(0.5)
        pool.inactivate(name)


class ActiveRequestsPoolTest(unittest.TestCase):
    """
    This is the best way I've found to test if ActiveRequestsPool does what it should.
    Although I'm not sure if it's the ideal approach, the tests reflect actual behavior
    as long as the worker is constructed properly - by calling both `pool.activate` and
    `pool.inactivate`.
    Inputs on the subject will be really appreciated :)
    """
    @classmethod
    def setUpClass(cls):
        cls.pool = ActiveRequestsPool()
        cls.sem = multiprocessing.Semaphore(MAX_REQUESTS_PER_SEC)
        cls.queue = multiprocessing.Queue()

    def test_activate_should_respect_semaphore_limit(self):
        times = 13
        args = (self.sem, self.pool)
        jobs = [
            multiprocessing.Process(target=fake_worker, name="worker{}".format(i), args=args)
            for i in range(times)
        ]
        for job in jobs:
            job.start()

        for job in jobs:
            job.join()
            self.assertLessEqual(len(self.pool.active), MAX_REQUESTS_PER_SEC)

        self.assertEqual(namespace.worker_called_times, times)

    def test_worker_should_feed_queue(self):
        times = 2
        args = (self.sem, self.pool, self.queue, 0, 300)
        jobs = [
            multiprocessing.Process(target=worker, name="worker{}".format(i), args=args)
            for i in range(times)
        ]

        for job in jobs:
            job.start()

        for job in jobs:
            job.join()
            resultn = self.queue.get()
            self.assertEqual(len(resultn), 300)
            self.assertIn("category_id", resultn[0].keys())
            self.assertIn("id", resultn[0].keys())
            self.assertIn("food_id", resultn[0].keys())
