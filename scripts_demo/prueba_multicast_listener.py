from icecream import ic
import time
from client.client import MulticastClient

_client = MulticastClient.get_client_multicast("127.0.0.1", 5000, role='listener', multicast_ip="230.0.0.1", multicast_port=2345)

while True:
    data = _client.get_data()
    if _client.end_of_multicast:
        break
    ic(data)
