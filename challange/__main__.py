from processor import SimpleMapReduce, map_worker, reduce_worker, DEFAULT_WAIT_TIMEOUT
from requests_pool import ActiveRequestsPool, worker
from semaphore import TimedSemaphore
from terminaltables import AsciiTable
import multiprocessing
import resource
import time


soft_limit, _ = resource.getrlimit(resource.RLIMIT_NPROC)
SEMAPHORE = multiprocessing.Semaphore(soft_limit)

MAX_REQS_PER_SECOND = 5
ENTRIES_PER_PAGE = 300


def show_data(data):
    food_table = [["food_id", "occurrences"]]
    values = data["food_id"].values()
    values.sort()
    values.reverse()
    i = 0
    for v1 in values:
        for k, v in data["food_id"].items():
            if v == v1:
                food_table.append([str(k), str(v)])
                i += 1
        if i >= 100:
            break

    table = AsciiTable(food_table)
    print table.table

    cat_table = [["category_id", "occurrences"]]
    for k, v in data["category_id"].items():
        cat_table.append([str(k), str(v)])
    table = AsciiTable(cat_table)
    print table.table


def request_workers(queue, jobs, total_resources):
    semaphore = TimedSemaphore(MAX_REQS_PER_SECOND, time=0.2)  # 0.2 * 5 = 1s
    request_pool = ActiveRequestsPool()
    offset = 0
    limit = ENTRIES_PER_PAGE
    step = ENTRIES_PER_PAGE
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
    jobs = []

    most_food = 100
    most_cat = 5
    mapreduce = SimpleMapReduce(map_worker, reduce_worker)
    keys_bymost = [("food_id", most_food), ("category_id", most_cat)]
    reduced = None
    processed_entries = 0
    times = 1000000
    total_resources = 500000000 / times

    try:
        with SEMAPHORE:
            for t in range(times):
                print("--> starting request workers")
                request_workers(queue, jobs, total_resources)
                print("> done starting request workers")

                print("--> processing everything on the queue")
                while not queue.empty():
                    reduced = mapreduce.processing(keys_bymost, queue)
                    processed_entries += ENTRIES_PER_PAGE
                print("> done processing queue")

                print("--> starting to process delayed jobs")
                for job in jobs:
                    if job.is_alive():
                        job.join(DEFAULT_WAIT_TIMEOUT)
                        if not queue.empty():
                            reduced = mapreduce.process(keys_bymost, queue)
                            processed_entries += ENTRIES_PER_PAGE
                        else:
                            print("-->>request failed to fill up queue! terminating anyway!")
                        job.terminate()
                        print("> done, going to next job")
    except KeyboardInterrupt:
        print("-> killing jobs, hang on!")
        for job in jobs:
            job.terminate()
        print("-> done! now processing result output, hang in there!")

    try:
        show_data(reduced)
        print("-> Total entries processed is ~{}".format(processed_entries))
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
