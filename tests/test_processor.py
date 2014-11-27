from challenge import requests_pool
from challenge.processor import SimpleMapReduce, map_worker, reduce_worker
from challenge.semaphore import TimedSemaphore
from collections import Counter
from tests.test_data import data, data1
import multiprocessing
import unittest


MAX_REQS_PER_SECOND = 5


class SimpleMapReduceTest(unittest.TestCase):

    def setUp(self):
        self.queue = multiprocessing.Queue()
        self.processor = SimpleMapReduce(map_worker, reduce_worker)

    def test_map_counts_how_many_entries_for_one_key(self):
        data = [{"food_id": 1, "id": 123},
                {"food_id": 1, "id": 231},
                {"food_id": 3, "id": 321},
                {"food_id": 7, "id": 132}]

        result = self.processor.map(data=data, keys=["food_id"])
        expected = {"food_id": {1: 2, 3: 1, 7: 1}}

        self.assertDictEqual(result, expected)

    def test_map_counts_entries_for_a_given_key_in_heterogeneous_dict(self):
        data = [{"food_id": 1, "category_id": 3, "id": 123},
                {"food_id": 1, "category_id": 3, "id": 321},
                {"food_id": 2, "category_id": 3, "id": 132},
                {"food_id": 7, "category_id": 3, "id": 231}]

        result = self.processor.map(data=data, keys=["food_id", "category_id"])
        expected = {"food_id": {1: 2, 2: 1, 7: 1}, "category_id": {3: 4}}

        self.assertDictEqual(result, expected)

    def test_map_counts_entries_considering_entry_id(self):
        data = [{"food_id": 1, "category_id": 3, "id": 123},
                {"food_id": 1, "category_id": 3, "id": 123},
                {"food_id": 2, "category_id": 3, "id": 132},
                {"food_id": 2, "category_id": 6, "id": 321},
                {"food_id": 7, "category_id": 3, "id": 231}]

        result = self.processor.map(data=data, keys=["food_id", "category_id"])
        expected = {"food_id": {1: 1, 2: 2, 7: 1}, "category_id": {3: 3, 6: 1}}

        self.assertDictEqual(result, expected)

    def test_map_reduce_works_with_queue(self):
        self.queue.put(data)

        result = self.processor.map(data=self.queue.get(), keys=["food_id"])
        result = self.processor.reduce(result["food_id"], by_most=5)
        expected = {100594: 27, 100584: 25, 100509: 16, 100515: 15, 100508: 12, 100510: 12}

        self.assertDictEqual(result, expected)

    def test_process_works_with_multiple_queue_results(self):
        # first queue result
        self.queue.put(data)
        result = self.processor.process([("food_id", 5)], self.queue)
        expected = {100594: 27, 100584: 25, 100509: 16, 100515: 15, 100508: 12, 100510: 12}
        self.assertDictEqual(result["food_id"], expected)

        # second queue result
        self.queue.put(data1)
        result = self.processor.process([("food_id", 5)], self.queue)
        expected = {100594: 54, 100584: 51, 100509: 32, 100515: 30, 100508: 25, 100510: 25}
        self.assertDictEqual(result["food_id"], expected)

    def test_reduce_filters_using_by_most(self):
        mapping = {"food_id": {8: 15, 2: 9, 7: 2, 14: 22, 93: 10, 20: 8},
                   "category_id": {3: 4, 9: 5, 11: 20}}

        result = self.processor.reduce(mapping=mapping["food_id"], by_most=5)
        expected = {14: 22, 8: 15, 93: 10, 2: 9, 20: 8}
        self.assertDictEqual(Counter(result), expected)


class SimpleMapReduceRequestPoolTest(unittest.TestCase):

    def setUp(self):
        self.queue = multiprocessing.Queue()
        self.mapreduce = SimpleMapReduce(map_worker, reduce_worker)
        self.pool = requests_pool.ActiveRequestsPool()
        self.sem = TimedSemaphore(MAX_REQS_PER_SECOND)

    def test_process_should_return_map_reduced_keys_result(self):
        times = 3
        offset = 0
        limit = 300
        step = 300
        jobs = []
        for t in range(times):
            args = (self.sem, self.pool, self.queue, offset, limit)
            p = multiprocessing.Process(target=requests_pool.worker, args=args)
            jobs.append(p)
            p.start()
            offset = limit
            limit += step

        for job in jobs:
            job.join()

        for t in range(times):
            reduced = self.mapreduce.process([("food_id", 5), ("category_id", 10)], self.queue)

        expected_food = {100594: 81, 100584: 77, 100509: 48, 100515: 45, 100508: 38, 100510: 38}
        self.assertDictEqual(reduced["food_id"], expected_food)
