from collections import Counter
from multiprocessing import Manager, Process
from Queue import Empty as EmptyQueue


DEFAULT_WAIT_TIMEOUT = 15  # huge timeout due to poor internet connection


class SimpleMapReduce(object):

    data = None

    def __init__(self, map_worker, reduce_worker):
        self.manager = Manager()
        self.namespace = self.manager.Namespace()
        self.namespace.mapping = None
        self.namespace.reduced = None
        self.map_worker = map_worker
        self.reduce_worker = reduce_worker
        self.reduced_bykey = {}

    def _remove_repeated(self, data):
        # new_data = {}
        # for k, v in data.items():
        #     if v not in new_data.values():
        #         new_data[k] = v

        return data

    def map(self, data, keys):
        """
        Maps data to an {id: times} dictionary where times is the
        occurrences of each key in `keys` list.
        Returns a list of the resulting dicts.
        """
        jobs = []
        mapping = {}
        for key in keys:
            result = self.manager.list()
            mapping[key] = result
            # wish I could use multiprocessing.Pool, but it doesn't repasses KeyboardInterrupt
            # up into the stack, making it impossible to treat it properly
            job = Process(target=self.map_worker, args=(data, key, result))
            job.start()
            jobs.append(job)

        for job in jobs:
            job.join(DEFAULT_WAIT_TIMEOUT)
            job.terminate()

        for key in keys:
            mapping[key]
            mapping[key] = Counter(mapping[key])
            mapping[key] = self._remove_repeated(mapping[key])

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
        p = Process(target=reduce_worker, args=(mapping, by_most, self.namespace))
        p.start()
        p.join(DEFAULT_WAIT_TIMEOUT)
        p.terminate()

        return self.namespace.reduced

    def process(self, keys_by_most, queue):
        """
        Calls self.map passing queue.get() and then reduce the results.

        keys_by_most: list of tuples with each key and it's repetition value, e.g.:
        self.process([("food_id", 5), ("category_id", 10)])
        """
        keys = []
        for key, _ in keys_by_most:
            keys.append(key)

        print("start processing request")
        try:
            # queue operations may block the program if underlying pipe is full
            # see http://bugs.python.org/issue8237
            mapping = self.map(queue.get(DEFAULT_WAIT_TIMEOUT), keys)
        except EmptyQueue:
            print("-->>found empty queue... moving on")
            return self.reduced_bykey

        for key, by_most in keys_by_most:
            self.merge_reduced(self.reduce(mapping[key], by_most), key, by_most)

        print("finished processing request.")

        return self.reduced_bykey

    def merge_reduced(self, new_reduced, key, by_most):
        """
        Merges a `new_reduced` dict with a previously reduced dict
        Updates `self.reduced_bykey` with new values.
        """
        if not (key in self.reduced_bykey.keys()):
            self.reduced_bykey[key] = {}

        for k_id, v in new_reduced.items():
            if k_id in self.reduced_bykey[key]:
                self.reduced_bykey[key][k_id] += v
            else:
                self.reduced_bykey[key][k_id] = v

        self.reduced_bykey[key] = self.reduce(self.reduced_bykey[key], by_most)
        return self.reduced_bykey


def map_worker(data, key, result):
    p = Process(target=map_fn, args=(data, key, result))
    p.start()
    p.join(DEFAULT_WAIT_TIMEOUT)
    p.terminate()


def map_fn(data, key, result):
    """
    Searches for `key` in the dict `obj`.
    Returns the value for `key`.

    It's not possible to use a multiprocessing.manager.dict() in here to handle results,
    I wanted to use nested dictionaries, but check out this "behavior": http://bugs.python.org/issue6766.
    The nested dict usage would allow the map function to map more than one key per time,
    while I don't find any better, I'm calling pool.map once for each key
    """
    entries_id = []
    for obj in data:
        if key in obj.keys() and obj["id"] not in entries_id:
            result.append(obj[key])
            entries_id.append(obj["id"])


def reduce_worker(mapping, by_most, namespace):
    p = Process(target=reduce_fn, args=(mapping, by_most, namespace))
    p.start()
    p.join(DEFAULT_WAIT_TIMEOUT)
    p.terminate()


def reduce_fn(objs, by_most, namespace):
    values = objs.values()
    values.sort()
    biggest_values = values[-by_most:]

    result = {}
    for k, v in objs.items():
        if v in biggest_values:
            result[k] = v

    namespace.reduced = result
