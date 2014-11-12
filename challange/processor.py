import itertools
from collections import Counter
from multiprocessing import Pool, Manager, Process


POOL_PROCESSES = 5  # is 5 good enough?


class SimpleMapReduce(object):

    data = None

    def __init__(self, worker, reduce_fn):
        self.namespace = Manager().Namespace()
        self.namespace.mapping = None
        self.worker = worker
        self.reduce_fn = reduce_fn

    def map(self, data, key):
        """
        Maps data to an {id: times} dictionary where times is the
        occurrences of `key`.
        Returns a list of those dicts.
        """
        if not data:
            raise ValueError("You should first set SimpleMapReduce().data")
        p = Process(target=self.worker, args=(data, key, self.namespace))
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
            by_most = 100
            simple_map_reduce.reduce(mapping, by_most)
            {432: 30, 876: 21}  # until the 100th

        Returns the filtered dict where the key is the key id (such as the category
        id) and the value is the number of times it was present on the data.
        """
        pass


def worker(data, key, namespace):
    pool = Pool(processes=POOL_PROCESSES)
    args = itertools.izip(data, itertools.repeat(key))
    mapping = pool.map(wrapper, args, chunksize=200)  # is this chunksize good?
    pool.terminate()
    pool.join()
    namespace.mapping = mapping


def wrapper(obj_key):
    return map_fn(*obj_key)


def map_fn(obj, key):
    if key in obj.keys():
        return obj[key]
    return
