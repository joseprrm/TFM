import threading
import time

from icecream import ic

def loop(datasets):
    while True:
        now = time.time()
        for name, dataset in datasets.items():
            if dataset.optimized is True:
                paths_to_delete = []
                for path, value in dataset.partition_cache.items():
                    access_time = value[1]
                    difference = now - access_time
                    if difference > 20:
                        paths_to_delete.append(path)

                for path in paths_to_delete:
                    del dataset.partition_cache[path]
        time.sleep(0.5)

def start(datasets):
    thread = threading.Thread(target=loop, args=(datasets,), daemon=True)
    thread.start()
