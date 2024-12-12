import time
from client.client import Client

_client = Client.get_client("127.0.0.1", 5000, method="pickle", channel_method='websocket')
dataset = _client.get_dataset('big_partitions_dask')

time_start = time.perf_counter()
for e in dataset.row_iterator(0, 10000):
    pass
time_end = time.perf_counter()
_client.close()

time_seconds = (time_end - time_start)
print(f"{time_seconds}")
