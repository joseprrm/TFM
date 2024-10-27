from client import Client
_client = Client("127.0.0.1", 5000)
dataset_names = _client.list_datasets()
print(dataset_names)
