"""
Gets the data set to be analyzed, piece by piece.
"""
import requests


LIFESUM_API_FOODSTATS = "https://api.lifesum.com/v1/foodipedia/foodstats?offset={0}&limit={1}"


def data_chunk(offset, limit):
    """
    Gets a list of foodstats objects from lifesum API.
    offset: start value for food id items.
    limit: number of items to return in one request.
    Returns a list of json objects, e.g:
        [
            {
                food_id: 1,
                id: 1,
                category_id: 33
            },
            {
                food_id: 2,
                id: 2,
                category_id: 36
            },  # ...
        ]
    """
    url = LIFESUM_API_FOODSTATS.format(offset, limit)
    response = requests.get(url)

    resp_json = response.json()
    if "response" in resp_json.keys():
        return resp_json["response"]

    return resp_json
