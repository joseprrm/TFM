import json
from abc import ABC, abstractmethod

from icecream import ic
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
        # sometimes websocket.recv returns bytes, sometimes str
        # encode if is a str
        if isinstance(response, str):
            response = response.encode('utf-8')
        return response

    def close(self):
        self.websocket.close()

mapping = {'http': HTTPChannel, 'websocket': WebsocketChannel}
