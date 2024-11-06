from icecream import ic
import time
from client.client import Client

_client = Client("127.0.0.1", 5000)
dataset_names = _client.list_datasets()
l = []
ds = _client.get_dataset('big_csv_int_1g_split')

start = time.time()

batch_size = 10
for i in range(0, 50000, batch_size):
    data = ds[range(i, i + batch_size-1), "C"]
    l.append(data)

print(l)
end = time.time()
print(end-start)
