from client.client import MulticastListenerClient
_client = MulticastListenerClient.get_client(
      "127.0.0.1", 5000, 
      multicast_ip="230.0.0.1", multicast_port=3456, method='json')
while True:
    data = _client.get_data()
    if _client.end_of_multicast:
        break
    print(data)
