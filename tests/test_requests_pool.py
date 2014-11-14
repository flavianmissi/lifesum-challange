import unittest
import multiprocessing
from challange.requests_pool import ActiveRequestsPool, MAX_REQUESTS_PER_SEC
import time


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

    def test_activate_should_respect_semaphore_limit(self):
        sem = multiprocessing.Semaphore(MAX_REQUESTS_PER_SEC)
        pool = ActiveRequestsPool()

        times = 13
        jobs = [
                   multiprocessing.Process(target=fake_worker, name="worker{}".format(i), args=(sem, pool))
                   for i in range(times)
               ]
        for job in jobs:
            job.start()

        for job in jobs:
            job.join()
            self.assertLessEqual(len(pool.active), MAX_REQUESTS_PER_SEC)

        self.assertEqual(namespace.worker_called_times, times)

