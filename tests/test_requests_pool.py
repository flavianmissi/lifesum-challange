import unittest
import multiprocessing
from challange.requests_pool import ActiveRequestsPool, MAX_REQUESTS_PER_SEC, worker, make_requests
import time
from mock import patch


namespace = multiprocessing.Manager().Namespace()
namespace.worker_called_times = 0


def fake_worker(sem, pool):
    name = multiprocessing.current_process().name
    with sem:
        pool.activate(name)
        # should actually be doing some work
        time.sleep(1)
        namespace.worker_called_times += 1
        pool.inactivate(name)


class ActiveRequestsPoolTest(unittest.TestCase):
    """
    This is the best way I've found to test if ActiveRequestsPool does what it should.
    Although I'm not sure if it's the ideal approach, the tests reflect actual behavior
    as long as the worker is constructed properly - by calling both `pool.activate` and
    `pool.inactivate`.
    Inputs on the subject will be really appreciated :)
    """
    def setUp(self):
        self.pool = ActiveRequestsPool()
        self.sem = multiprocessing.Semaphore(MAX_REQUESTS_PER_SEC)

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
        queue = multiprocessing.Queue()
        args = (self.sem, self.pool, queue, 0, 300)
        jobs = [
            multiprocessing.Process(target=worker, name="worker{}".format(i), args=args)
            for i in range(times)
        ]

        for job in jobs:
            job.start()

        for job in jobs:
            job.join()
            resultn = queue.get()
            self.assertEqual(len(resultn), 300)
            self.assertIn("category_id", resultn[0].keys())
            self.assertIn("id", resultn[0].keys())
            self.assertIn("food_id", resultn[0].keys())

    @patch("challange.requests_pool.data_chunk")
    def test_make_requests_should_start_worker_and_feed_queue(self, data_chunk_mock):
        jobs = make_requests(max_limit=30000)
