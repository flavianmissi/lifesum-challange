from dataset import data_chunk
from processor import SimpleMapReduce, worker


def main():
    p = SimpleMapReduce(worker, None)
    results = {}
    step = 300
    offset = 0
    for limit in range(step, 500000, step):  # for now
        p.data = data_chunk(offset, limit)
        result = p.map(key="food_id")

        results = dict(results.items() + result.items())
        offset = limit

    print(results)

main()
