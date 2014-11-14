import unittest
from challange.processor import SimpleMapReduce, worker


class SimpleMapReduceTest(unittest.TestCase):

    def setUp(self):
        self.processor = SimpleMapReduce(worker, None)

    def test_map_counts_how_many_entries_for_one_key(self):
        data = [{"food_id": 1}, {"food_id": 1}, {"food_id": 3}, {"food_id": 7}]

        result = self.processor.map(data=data, keys=["food_id"])
        expected = {1: 2, 3: 1, 7: 1}

        self.assertDictEqual(result, expected)

    def test_map_counts_entries_for_a_given_key_in_heterogeneous_dict(self):
        data = [{"food_id": 1}, {"food_id": 1}, {"food_id": 2,"category_id": 3}, {"food_id": 7}]
        self.processor.data = data

        result = self.processor.map(data=data, keys=["food_id"])
        expected = {1: 2, 2: 1, 7: 1}

        self.assertDictEqual(result, expected)
