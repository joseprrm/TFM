import json
from abc import ABC, abstractmethod

import requests
from websockets.sync.client import connect

class Channel(ABC):
    @abstractmethod
    def make_petition(self, petition):
        pass

    @abstractmethod
    def close(self):
        pass

class HTTPChannel(Channel):
    def make_petition(self, petition):
        url, query = petition
        return requests.get(url, json=query).content

    def close(self):
        pass

class WebsocketChannel(Channel):
    def __init__(self):
        self.websocket = connect("ws://localhost:30004")
#        super().__init__()

    def make_petition(self, petition):
        # just ignore the url
        url, query = petition
        self.websocket.send(json.dumps(query))
        response = self.websocket.recv()
        return response

    def deserialize(self, response):
        data_pickled = base64.b64decode(response)
        data = pickle.loads(data_pickled)
        return data

    def close(self):
        self.websocket.close()

mapping = {'http': HTTPChannel, 'websocket': WebsocketChannel}
