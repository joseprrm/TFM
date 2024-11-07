import json
from abc import ABC, abstractmethod

from icecream import ic
import requests
from websockets.sync.client import connect
import socket

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

class TCPChannel(Channel):
    def __init__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ip="127.0.0.1"
        port=8000
        self.socket.connect((ip, port))
        #ic('connect socket')

    def make_petition(self, petition):
        # just ignore the url
        url, query = petition
        self.socket.sendall(json.dumps(query).encode('utf-8'))
        response = self.socket.recv(4096)
        return response

    def close(self):
        #ic('Closing client socket')
        self.socket.close()


mapping = {'http': HTTPChannel, 'websocket': WebsocketChannel, 'tcp': TCPChannel}
