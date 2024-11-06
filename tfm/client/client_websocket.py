import json
import base64
import pickle

from websockets.sync.client import connect

from .client import Client

class ClientWebsocket(Client):
    def __init__(self, *args):
        self.websocket = connect("ws://localhost:30004")
        super().__init__(*args)

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
