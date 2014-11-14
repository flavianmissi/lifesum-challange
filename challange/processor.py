import itertools
from collections import Counter
from multiprocessing import Pool, Manager, Process


POOL_PROCESSES = 5  # is 5 good enough?


class SimpleMapReduce(object):

    data = None

    def __init__(self, worker, reduce_fn):
        self.manager = Manager()
        self.namespace = self.manager.Namespace()
        self.namespace.mapping = None
        self.worker = worker
        self.reduce_fn = reduce_fn
        self.results = None

    def map(self, data, keys):
        """
        Maps data to an {id: times} dictionary where times is the
        occurrences of each key in `keys` list.
        Returns a list of the resulting dicts.
        """
        if not data:
            raise ValueError("You should first set SimpleMapReduce().data")
        p = Process(target=self.worker, args=(data, keys, self.namespace))
        p.start()
        p.join()
        return Counter(self.namespace.mapping)

    def reduce(self, mapping, by_most):
        """
        Reduces `mapping` by filtering the highest values until `by_most`.
        mapping: the {id: times} map
        by_most: most numbers of occurrences

        Example::
            mapping = "category_id"
            by_most = 5
            simple_map_reduce.reduce(mapping, by_most)
            {432: 30, 876: 21}  # until the 5th

        Returns the filtered dict where the key is the key id (such as the category
        id) and the value is the number of times it was present on the data.
        """
        pass

    def _get_results(self):
        if not self.results:
            self.results = self.manager.dict()
        return self.results


def worker(data, key, namespace):
    pool = Pool(processes=POOL_PROCESSES)
    args = itertools.izip(data, itertools.repeat(key))
    mapping = pool.map(wrapper, args, chunksize=300)  # is this chunksize good?
    pool.terminate()
    pool.join()
    namespace.mapping = mapping


def wrapper(obj_keys):
    """
    An "alternative" way to pass more than one parameter into a map function
    """
    return map_fn(*obj_keys)


def map_fn(obj, keys):
    """
    Searches for `keys` in the dict `obj`.
    Returns the value for each key in `keys`.

    #TODO: add +1 for each obj[key] found in a {key: count} shared dict
    """
    for key in keys:
        if key in obj.keys() and key in keys:
            return obj[key]
    return
