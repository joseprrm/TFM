from icecream import ic
import time
from client.client import MulticastClient

_client = MulticastClient.get_client_multicast("127.0.0.1", 5000, role='requester', multicast_ip="230.0.0.1", multicast_port=2345)
dataset_names = _client.list_datasets()
l = []
ds = _client.get_dataset('iris')
start = time.time()
for i in ds.filter_columns('species'):
    l.append(i)
end = time.time()
print(start-end)
_client.close()
