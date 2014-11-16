import unittest
from mock import patch, Mock

from challange import dataset


class DataSetTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.json_mock = {
                "meta": {
                    "limit": 300,
                    "code": 200,
                    "next_offset": 4591,
                    "offset": 0
                },
                "response":[
                    {"food_id": 1,"id": 1,"category_id": 33},
                    {"food_id": 3,"id": 3,"category_id": 36}
                ]
            }
        cls.response_mock = Mock()
        cls.response_mock.json.return_value = cls.json_mock

    @patch("challange.dataset.requests")
    def test_data_chunk_calls_requests_with_given_offset_and_limit(self, requests_mock):
        requests_mock.get.return_value = self.response_mock

        dataset.data_chunk(0, 300)  # ignore return
        requests_mock.get.assert_called_with(dataset.LIFESUM_API_FOODSTATS.format(0, 300))

    @patch("challange.dataset.requests")
    def test_data_chunk_returns_only_json_relevant_data(self, requests_mock):
        requests_mock.get.return_value = self.response_mock

        data = dataset.data_chunk(0, 300)
        self.assertIsNotNone(data)
        expected = self.json_mock["response"]
        self.assertListEqual(data, expected)
