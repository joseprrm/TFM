from icecream import ic
import timeit
from client.client import Client
from client.client_websocket import ClientWebsocket

#_client = Client.get_client("127.0.0.1", 5000, method="pickle_base64")
_client = ClientWebsocket("127.0.0.1", 5000)
dataset_names = _client.list_datasets()
l = []
ds = _client.get_dataset('big_csv_int_1g_split')
for i in ds.filter_columns('C'):
    l.append(i)
_client.close()
ic(l)
ic(len(l))
