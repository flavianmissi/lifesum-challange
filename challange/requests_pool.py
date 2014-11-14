import multiprocessing


MAX_REQUESTS_PER_SEC = 5


class ActiveRequestsPool(object):

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


def worker(semaphore, req_pool):
    """
    Make a request respecting the `semaphore` value.
    """
    pass
