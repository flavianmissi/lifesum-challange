from processor import SimpleMapReduce, map_worker, reduce_worker, DEFAULT_WAIT_TIMEOUT
from requests_pool import ActiveRequestsPool, worker
from semaphore import TimedSemaphore
from terminaltables import AsciiTable
import multiprocessing
import resource
import time


_, hard_limit = resource.getrlimit(resource.RLIMIT_NPROC)
SEMAPHORE = multiprocessing.Semaphore(hard_limit)

MAX_REQS_PER_SECOND = 5


def remove_repetitions(data):
    values = set(data.values())
    new_data = {}
    for value in values:
        for k, v in data.items():
            if v == value and v not in new_data.values():
                new_data[k] = v

    return new_data


def show_data(data):
    # TODO: use set (for values) when printing food_ids,
    # the algorithm adds repeated values even though the end result
    # is higher than the requested (100 in this case)
    food_table = [["food_id", "occurrences"]]
    food_id_data = remove_repetitions(data["food_id"])
    for k, v in food_id_data.items():
        food_table.append([str(k), str(v)])
    table = AsciiTable(food_table)
    print table.table

    cat_table = [["category_id", "occurrences"]]
    for k, v in data["category_id"].items():
        cat_table.append([str(k), str(v)])
    table = AsciiTable(cat_table)
    print table.table


def request_workers(queue, jobs):
    semaphore = TimedSemaphore(MAX_REQS_PER_SECOND, time=0.2)  # 0.2 * 5 = 1s
    request_pool = ActiveRequestsPool()
    total_resources = 5000
    offset = 0
    limit = 300
    step = 300
    args = (semaphore, request_pool, queue, offset, limit)
    with SEMAPHORE:
        while limit <= total_resources:
            p = multiprocessing.Process(target=worker, args=args)
            jobs.append(p)
            p.start()
            offset = limit
            limit += step


def main():
    queue = multiprocessing.Queue()
    # queue operations may block the program if underlying pipe is full
    # see http://bugs.python.org/issue8237
    # we could get small chunks of data so the queue is never full
    # why the API isn't coherent when I try to get a smaller chunk?
    # as an exaple, with offset = 0 and limit = 150 search for the following item:
    # food_id: 95302,
    # id: 3485,
    # category_id: 135
    # you'll find it once, now change the offset to 151 and the limit to 300,
    # search for the same pattern again, you'll also find it once.
    # so we've seen the pattern twice so far, right?
    # now change offset = 0 and limit = 300, search for the pattern again,
    # you'll find it only once. what's that?
    jobs = []

    most_food = 100
    most_cat = 5
    mapreduce = SimpleMapReduce(map_worker, reduce_worker)
    keys_bymost = [("food_id", most_food), ("category_id", most_cat)]
    reduced = None

    try:
        with SEMAPHORE:
            print("starting request workers")
            request_workers(queue, jobs)
            print("done starting request workers")

            print("processing everything on the queue")
            while not queue.empty():
                reduced = mapreduce.processing(keys_bymost, queue)
            print("done processing queue")

            print("starting to process delayed jobs")
            for job in jobs:
                if job.is_alive():
                    job.join(DEFAULT_WAIT_TIMEOUT)
                    if not queue.empty():
                        reduced = mapreduce.process(keys_bymost, queue)
                    else:
                        print("-->>request failed to fill up queue! terminating anyway!")
                    job.terminate()
                    print("done, going to next job")
    except KeyboardInterrupt:
        print("-> killing jobs, hang on!")
        for job in jobs:
            job.terminate()
        print("-> done! now processing result output, hang in there!")

    try:
        show_data(reduced)
    except Exception:
        print("--> Oops! No data to show, try again and wait some more time please.")


try:
    main()
except KeyboardInterrupt as e:
    import sys
    print("killing softly...")
    time.sleep(1)
    raise e
    sys.exit(0)
