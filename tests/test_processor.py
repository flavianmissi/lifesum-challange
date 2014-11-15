import unittest
from challange.processor import SimpleMapReduce, map_worker, reduce_worker


class SimpleMapReduceTest(unittest.TestCase):

    def setUp(self):
        self.processor = SimpleMapReduce(map_worker, reduce_worker)

    def test_map_counts_how_many_entries_for_one_key(self):
        data = [{"food_id": 1}, {"food_id": 1}, {"food_id": 3}, {"food_id": 7}]

        result = self.processor.map(data=data, keys=["food_id"])
        expected = {"food_id": {1: 2, 3: 1, 7: 1}}

        self.assertDictEqual(result, expected)

    def test_map_counts_entries_for_a_given_key_in_heterogeneous_dict(self):
        data = [{"food_id": 1,"category_id": 3},
                {"food_id": 1,"category_id": 3},
                {"food_id": 2,"category_id": 3},
                {"food_id": 7,"category_id": 3}]
        self.processor.data = data

        result = self.processor.map(data=data, keys=["food_id", "category_id"])
        expected = {"food_id": {1: 2, 2: 1, 7: 1}, "category_id": {3: 4}}

        self.assertDictEqual(result, expected)

    def test_reduce_filters_using_by_most(self):
        mapping = {"food_id": {8: 15, 2: 9, 7: 2, 14: 22, 93: 10, 20: 8},
                   "category_id": {3: 4, 9: 5, 11: 20}}

        result = self.processor.reduce(mapping=mapping["food_id"], by_most=5)
        expected = {14: 22, 8: 15, 93: 10, 2: 9, 20: 8}
        self.assertDictEqual(result, expected)
