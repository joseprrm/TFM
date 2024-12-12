from client.client import MulticastRequesterClient
_client = MulticastRequesterClient.get_client( 
      "127.0.0.1", 5000, 
      multicast_ip="230.0.0.1", multicast_port=3456, method='json')
ds = _client.get_dataset('iris')
for i in ds.filter_columns('species'):
    pass
_client.close()
