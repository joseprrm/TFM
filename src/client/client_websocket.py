import json
import base64
import pickle

from websockets.sync.client import connect

from client.client import Client

class ClientWebsocket(Client):
    def __init__(self, *args):
        self.websocket = connect("ws://localhost:30004")
        super().__init__(*args)

    def read_csv(self, dataset_name, **kwargs):
        """
        Possible arguments:
        row: int
        column: str
        columns: [str, ...]
        rows: (start, end)
        step(to be used with rows): int
        random: boolean o None
        """
        #url = f"{self.base_url}/datasets/{dataset_name}"

        # This deletes the elements that are None,
        # so that we don't send them to the server in the json
        query = {k:v for k,v in kwargs.items() if v is not None}
        query = self.add_method(query)

        self.websocket.send(json.dumps(query))
        response = self.websocket.recv()

        contents = self.deserialize(response)
        return contents

    def deserialize(self, response):
        data_pickled = base64.b64decode(response)
        data = pickle.loads(data_pickled)
        return data

    def close(self):
        self.websocket.close()
