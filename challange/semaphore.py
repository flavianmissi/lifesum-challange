from threading import Timer, _Semaphore


class TimedSemaphore(_Semaphore):
    """
       multiprocess.Semaphore extension, implements a TimedSemaphore.

       Sometimes we need to access resources with a time restriction, e.g. an API
       has a maximum limit of N requests per second, using this special semaphore,
       this can be accomplished easily.
    """

    def __init__(self, value=1, verbose=None, time=1):
        """
        Receives same params as it's parent Semaphore, but includes the time
        range in seconds each value should respect.
        """
        super(TimedSemaphore, self).__init__(value, verbose)
        if time < 0:
            raise ValueError("time period must be grater than 0.")
        self._time = time

    def release(self, *args, **kwargs):
        release_func = super(TimedSemaphore, self).release
        release_timer = Timer(self._time,
                              release_func,
                              args=args,
                              kwargs=kwargs)
        release_timer.start()
