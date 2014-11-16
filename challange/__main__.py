from processor import SimpleMapReduce, map_worker, reduce_worker
from requests_pool import ActiveRequestsPool, worker
import multiprocessing


MAX_REQS_PER_SECOND = 5


def main():
    max_limit = 5000
    queue = multiprocessing.Queue()
    semaphore = multiprocessing.Semaphore(MAX_REQS_PER_SECOND)
    request_pool = ActiveRequestsPool()
    offset = 0
    limit = 300
    step = 300
    times = int(round(max_limit / float(step)))  # divides per step
    jobs = []
    for t in range(times):
        args = (semaphore, request_pool, queue, offset, limit)
        p = multiprocessing.Process(target=worker, args=args)
        jobs.append(p)
        p.start()
        offset = limit
        limit += step

    # wait for at least 5 jobs to complete
    # this way we don't pass through the queue when it is not
    # filled in yet.
    for i in range(5):
        jobs[0].join()

    most_food = 100
    most_cat = 5
    mapreduce = SimpleMapReduce(map_worker, reduce_worker)
    keys_bymost = [("food_id", most_food), ("category_id", most_cat)]
    reduced = None
    while not queue.empty():
        reduced = mapreduce.process(keys_bymost, queue)

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
