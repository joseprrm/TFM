import sys
import time
from client.client import Client

initial_row = int(sys.argv[1])
number_of_rows = int(sys.argv[2])

_client = Client.get_client("127.0.0.1", 5000, method="pickle", channel_method='http')
dataset = _client.get_dataset('big_partitions')

time_start = time.perf_counter_ns()
dataset[range(initial_row, initial_row+number_of_rows)]
time_end = time.perf_counter_ns()

time_miliseconds = (time_end - time_start) / 1e6
time_seconds = (time_end - time_start) / 1e9
print(f"{time_miliseconds},")
