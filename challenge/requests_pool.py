import multiprocessing
from dataset import data_chunk


class ActiveRequestsPool(object):
    """
    #TODO: Should control not only by active, but active per some time
    range.
    This means this class should handle the inactivation of processes on its own.
    """

    def __init__(self):
        self.manager = multiprocessing.Manager()
        self.active = self.manager.list()
        self.lock = multiprocessing.Lock()

    def activate(self, process_name):
        """
        Adds `process_name` into list of active processes.
        """
        with self.lock:
            self.active.append(process_name)

    def inactivate(self, process_name):
        """
        Removes `process_name` from list of active processes.
        """
        with self.lock:
            self.active.remove(process_name)


def worker(semaphore, req_pool, result_queue, offset, limit):
    """
    Make a request respecting the `semaphore` value.
    """
    name = multiprocessing.current_process().name
    with semaphore:
        req_pool.activate(name)
        data = data_chunk(offset, limit)
        result_queue.put(data, 10)
        req_pool.inactivate(name)
