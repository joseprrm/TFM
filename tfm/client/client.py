import io
import zlib
import pickle
import json
import base64
import requests
import pandas
from icecream import ic

from utils import ProgrammingError

from .dataset import Dataset
from . import serialization


class Client():
    @classmethod
    def get_client(cls, *args, method="pickle_base64"):
        """
        Factory method to return different types of client.
        """
        match method:
            case "pickle_base64":
                return Client(*args, method, serialization.PickleBase64Serializer())
            case "pickle":
                return Client(*args, method, serialization.PickleSerializer())
            case "pickle_compressed":
                return Client(*args, method, serialization.PickleCompressedSerializer())
            case "json":
                return Client(*args, method, serialization.JSONSerializer())
            case "websocket":
                from .client_websocket import ClientWebsocket
                return ClientWebsocket(*args, "pickle_base64", serialization.PickleBase64Serializer())
            case _:
                raise ProgrammingError

    def __init__(self, server_address, server_port, method, serializer):
        """
        server_address: a string with the IP or hostname of the server
        server_port: intenger with the port number
        """
        self.server_address = server_address
        self.server_port = str(server_port)
        self.base_url = f"http://{server_address}:{server_port}/"
        self.method = method
        self.serializer = serializer

    def get_dataset(self, dataset_name):
        return Dataset(dataset_name, self)

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
        url = f"{self.base_url}/datasets/{dataset_name}"

        # This deletes the elements that are None,
        # so that we don't send them to the server in the json
        query = {k:v for k,v in kwargs.items() if v is not None}
        query = self._add_method(query)

        petition = (url, query)
        response = self._make_petition(petition)

        contents = self.serializer.deserialize(response)
        return contents

    def _make_petition(self, petition):
        url, query = petition
        return requests.get(url, json=query).content

    def _add_method(self, query):
        query['method'] = self.method
        return query

    def list_datasets(self):
        url = f"{self.base_url}/datasets"
        response = requests.get(url)
        return response.text

    def number_of_rows(self, dataset_name):
        url = f"{self.base_url}/datasets/{dataset_name}/number_of_rows"
        response = requests.get(url)
        number_of_rows = response.json()
        return number_of_rows

    def column_names(self, dataset_name):
        url = f"{self.base_url}/datasets/{dataset_name}/column_names"
        response = requests.get(url)
        column_names = response.json()
        return column_names
