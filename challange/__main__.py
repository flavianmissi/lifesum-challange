from dataset import data_chunk
from processor import SimpleMapReduce, map_worker
import multiprocessing



def main():
    p = SimpleMapReduce(map_worker, None, multiprocessing.Queue())
    results = {"food_id": {}}
    step = 300
    for limit in range(step, 900, step):  # for now
        data = data_chunk()
        result = p.map(data, keys=["food_id"])

        for k, v in result.items():
            if k in results["food_id"].keys():
                results["food_id"][k] += v
            else:
                results["food_id"][k] = v

    print(results)

main()
