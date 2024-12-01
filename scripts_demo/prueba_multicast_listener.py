from icecream import ic
import time
from client.client import Client

_client = Client.get_client_multicast("127.0.0.1", 5000, mtype='multicast_listener', multicast_ip="230.0.0.1", multicast_port=2345, method="json")
while True:
    data = _client.get_data()
    ic(data)
