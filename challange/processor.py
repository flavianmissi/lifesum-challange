import itertools
from collections import Counter
from multiprocessing import Pool, Manager, Process


POOL_PROCESSES = 5  # is 5 good enough?


class SimpleMapReduce(object):

    data = None

    def __init__(self, map_worker, reduce_worker):
        self.namespace = Manager().Namespace()
        self.namespace.mapping = None
        self.namespace.reduced = None
        self.map_worker = map_worker
        self.reduce_worker = reduce_worker
        self.results = None

    def map(self, data, keys):
        """
        Maps data to an {id: times} dictionary where times is the
        occurrences of each key in `keys` list.
        Returns a list of the resulting dicts.
        """
        if not data:
            raise ValueError("You should first set SimpleMapReduce().data")
        p = Process(target=self.map_worker, args=(data, keys, self.namespace))
        p.start()
        p.join()
        mapping = {}
        for key in keys:
            mapping[key] = Counter(self.namespace.mapping[key])
        return mapping

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
        result = {}
        p = Process(target=reduce_worker, args=(mapping, by_most, self.namespace))
        p.start()
        p.join()
        return self.namespace.reduced


def map_worker(data, keys, namespace):
    pool = Pool(processes=POOL_PROCESSES)
    mapping = {}
    for key in keys:
        args = itertools.izip(data, itertools.repeat(key))
        mapping[key] = pool.map(wrapper, args, chunksize=300)
    pool.terminate()
    pool.join()
    namespace.mapping = mapping


def wrapper(obj_key):
    """
    An "alternative" way to pass more than one parameter into a map function
    """
    return map_fn(*obj_key)


def map_fn(obj, key):
    """
    Searches for `keys` in the dict `obj`.
    Returns the value for each key in `keys`.

    It's not possible to use a multiprocessing.manager.dict() in here,
    I wanted to use nested dictionaries, but check out this "behavior": http://bugs.python.org/issue6766.
    The nested dict usage would allow the map function to map more than one key per time,
    while I don't find anyway better, I'm calling pool.map once for each key ><
    """
    if key in obj.keys():
        return obj[key]
    return


def reduce_worker(mapping, by_most, namespace):
    p = Pool(processes=POOL_PROCESSES)
    result = p.apply(reduce_fn, (mapping, by_most))
    p.terminate()
    p.join()
    namespace.reduced = result


def reduce_fn(objs, by_most):
    values = objs.values()
    values.sort()
    values = values[-by_most:]

    result = {}
    for k, v in objs.items():
        if v in values:
            result[k] = v

    return result
