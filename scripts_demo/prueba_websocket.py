from icecream import ic
import time
from client.client import Client

_client = Client.get_client("127.0.0.1", 5000, method="pickle_base64", channel_method='websocket')
dataset_names = _client.list_datasets()
l = []
ds = _client.get_dataset('big_csv_int_1g_split')
start = time.time()
for i in ds.filter_columns('C'):
    l.append(i)
end = time.time()
print(start-end)
_client.close()
#ic(l)
#ic(len(l))
