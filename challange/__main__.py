from processor import SimpleMapReduce, map_worker, reduce_worker
from semaphore import TimedSemaphore
from requests_pool import ActiveRequestsPool, worker
import multiprocessing
import resource


soft_limit, _ = resource.getrlimit(resource.RLIMIT_NPROC)
SEMAPHORE = multiprocessing.Semaphore(soft_limit)

MAX_REQS_PER_SECOND = 5


def main():
    max_limit = 500000
    queue = multiprocessing.Queue()
    semaphore = TimedSemaphore(MAX_REQS_PER_SECOND)
    request_pool = ActiveRequestsPool()
    offset = 0
    limit = 300
    step = 300
    times = int(round(max_limit / float(step)))  # divides per step
    jobs = []

    most_food = 100
    most_cat = 5
    mapreduce = SimpleMapReduce(map_worker, reduce_worker)
    keys_bymost = [("food_id", most_food), ("category_id", most_cat)]
    reduced = None

    with SEMAPHORE:
        for t in range(times):
            args = (semaphore, request_pool, queue, offset, limit)
            p = multiprocessing.Process(target=worker, args=args)
            jobs.append(p)
            p.start()
            offset = limit
            limit += step

            if not queue.empty():
                reduced = mapreduce.process(keys_bymost, queue)

        #import pdb;pdb.set_trace()
        while not queue.empty():
            reduced = mapreduce.process(keys_bymost, queue)

        for job in jobs:
            if job.is_alive():
                job.join(5)
                if not queue.empty():
                    reduced = mapreduce.process(keys_bymost, queue)
                job.terminate()

    reduced_foodid_values = reduced["food_id"].values()
    reduced_catid_values = reduced["category_id"].values()

    reduced_foodid_values.sort()
    reduced_catid_values.sort()

    print("MOST {} FOOD_IDs:".format(most_food))
    print("FOOD_ID     OCCURRENCES")
    for v2 in reduced_foodid_values:
        for k, v in reduced["food_id"].items():
            if v == v2:
                print("{}       :  {}".format(k, v))
                break

    print("======================================================================================")
    print("MOST {} CATEGORY_IDs:".format(most_cat))
    print("CATEGORY_ID     OCCURRENCES")
    for v2 in reduced_catid_values:
        for k, v in reduced["category_id"].items():
            if v == v2:
                print("{}           :  {}".format(k, v))
                break


main()
