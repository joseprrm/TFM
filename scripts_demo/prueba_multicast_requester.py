from icecream import ic
import time
from client.client import Client

_client = Client.get_client_multicast("127.0.0.1", 5000, mtype='multicast_requester', multicast_ip="230.0.0.1", multicast_port=2345, method="json")
dataset_names = _client.list_datasets()
l = []
ds = _client.get_dataset('iris')
start = time.time()
for i in ds.filter_columns('species'):
    l.append(i)
end = time.time()
print(start-end)
_client.close()
