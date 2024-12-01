import requests
from icecream import ic

from utils import ProgrammingError

from .dataset import Dataset
import serialization
from . import channel


import asyncio

# TODO: context manager, using the close
class Client():
    @classmethod
    def get_client(cls, *args, method="pickle_base64", channel_method='http'):
        """
        Factory method to return different types of client.
        """

        serializer = serialization.mapping[method]()
        _channel = channel.mapping[channel_method]()

        return Client(*args, method, serializer, _channel)

    @classmethod
    def get_client_multicast(cls, *args, mtype, multicast_ip=None, multicast_port=None, method="pickle_base64", ):
        """
        Factory method to return different types of client.
        """

        serializer = serialization.mapping[method]()
        if mtype == 'multicast_requester':
            _channel = channel.mapping["http"]()
        if mtype == 'multicast_listener':
            _channel = channel.mapping["multicast_listener"](multicast_ip, multicast_port)

        return Client(*args, method, serializer, _channel, multicast_ip, multicast_port)

    @classmethod
    async def get_client_tcp_async(cls, *args, method="pickle_base64"):
        serializer = serialization.mapping[method]()
        ip = "127.0.0.1"
        port = 8000
        reader, writer = await asyncio.open_connection(ip, port)
        _channel = channel.mapping['tcpasync'](reader, writer)
        return Client(*args, method, serializer, _channel)

    def __init__(self, server_address, server_port, method, serializer, channel, multicast_ip=None, multicast_port=None):
        """
        server_address: a string with the IP or hostname of the server
        server_port: intenger with the port number
        """
        self.server_address = server_address
        self.server_port = str(server_port)
        self.base_url = f"http://{server_address}:{server_port}/"
        self.method = method
        self.serializer = serializer
        self.channel = channel
        self.multicast_ip = multicast_ip
        self.multicast_port = multicast_port

    def get_dataset(self, dataset_name):
        return Dataset(dataset_name, self)

    async def read_csv_async(self, dataset_name, **kwargs):
        url = f"{self.base_url}/datasets/{dataset_name}"

        # This deletes the elements that are None,
        # so that we don't send them to the server in the json
        query = {k:v for k,v in kwargs.items() if v is not None}
        query = self._add_method(query)
        query["dataset_name"] = dataset_name

        petition = (url, query)
        response = await self.channel.make_petition_async(petition)

        contents = self.serializer.deserialize(response)
        return contents

    def get_data(self):
        data = self.channel.read_data()
        data = self.serializer.deserialize(data)
        return data

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
        query["dataset_name"] = dataset_name

        if self.is_multicast():
            query = self._add_multicast(query)
            url = url + "/multicast"

        petition = (url, query)
        response = self.channel.make_petition(petition)

        if self.is_multicast():
            return

        contents = self.serializer.deserialize(response)
        return contents

    def _add_method(self, query):
        query['method'] = self.method
        return query

    def is_multicast(self):
        if self.multicast_ip and self.multicast_port:
            return True
        else:
            return False

    def _add_multicast(self, query):
        query['ip'] = self.multicast_ip
        query['port'] = self.multicast_port
        return query

    def close(self):
        self.channel.close()

    async def closeasync(self):
        await self.channel.close()

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
