from icecream import ic
import time
from client.client import Client

_client = Client.get_client("127.0.0.1", 5000, method="json", channel_method='tcp')
dataset_names = _client.list_datasets()
l = []
ds = _client.get_dataset('big_csv_int_1g_split')
start = time.time()
#for i in ds.filter_columns(['C','c', 'a', 'b', 'f']):
for i in ds.filter_columns(['C']):
#for i in ds:
    l.append(i)
end = time.time()
print(end-start)
_client.close()
