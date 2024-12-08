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
        serializer = serialization.mapping[method]()
        _channel = channel.mapping[channel_method]()
        return Client(*args, method, serializer, _channel)

    @classmethod
    async def get_client_tcp_async(cls, *args, method="pickle_base64"):
        serializer = serialization.mapping[method]()
        ip = "127.0.0.1"
        port = 8000
        reader, writer = await asyncio.open_connection(ip, port)
        _channel = channel.mapping['tcpasync'](reader, writer)
        return Client(*args, method, serializer, _channel)

    def __init__(self, server_address, server_port, method, serializer, channel):
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

    def read_csv(self, dataset_name, **kwargs):
        url = f"{self.base_url}/datasets/{dataset_name}"

        # This deletes the elements that are None,
        # so that we don't send them to the server in the json
        query = {k:v for k,v in kwargs.items() if v is not None}
        query = self._add_method(query)
        query["dataset_name"] = dataset_name

        petition = (url, query)
        response = self.channel.make_petition(petition)

        contents = self.serializer.deserialize(response)
        return contents

    def _add_method(self, query):
        query['method'] = self.method
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



class MulticastClient(Client):
    def __init__(self, server_address, server_port, method, serializer, channel, multicast_ip=None, multicast_port=None):
        """
        server_address: a string with the IP or hostname of the server
        server_port: intenger with the port number
        """
        self.multicast_ip = multicast_ip
        self.multicast_port = multicast_port
        self.end_of_multicast = False
        super().__init__(server_address, server_port, method, serializer, channel)

    def _add_multicast(self, query):
        query['ip'] = self.multicast_ip
        query['port'] = self.multicast_port
        return query

class MulticastRequesterClient(MulticastClient):
    @classmethod
    def get_client(cls, *args, multicast_ip=None, multicast_port=None, method="pickle_base64", ):
        """
        Factory method to return different types of client.
        """
        serializer = serialization.mapping[method]()
        _channel = channel.mapping["http"]()
        return MulticastRequesterClient(*args, method, serializer, _channel, multicast_ip, multicast_port)

    def read_csv(self, dataset_name, **kwargs):
        url = f"{self.base_url}/datasets/{dataset_name}/multicast"

        # This deletes the elements that are None,
        # so that we don't send them to the server in the json
        query = {k:v for k,v in kwargs.items() if v is not None}
        query = self._add_method(query)
        query["dataset_name"] = dataset_name

        query = self._add_multicast(query)

        petition = (url, query)
        response = self.channel.make_petition(petition)

    def send_multicast_end_message(self):
        url = f"{self.base_url}/multicast_end"
        query = {'method':self.method}
        query = self._add_multicast(query)
        petition = (url, query)
        response = self.channel.make_petition(petition)

    def close(self):
        self.send_multicast_end_message()
        self.channel.close()

class MulticastListenerClient(MulticastClient):
    @classmethod
    def get_client(cls, *args, multicast_ip=None, multicast_port=None, method="pickle_base64", ):
        """
        Factory method to return different types of client.
        """
        serializer = serialization.mapping[method]()
        _channel = channel.mapping["multicast_listener"](multicast_ip, multicast_port)
        return MulticastListenerClient(*args, method, serializer, _channel, multicast_ip, multicast_port)

    def get_data(self):
        data = self.channel.read_data()
        data = self.serializer.deserialize(data)
        if data == 'END_OF_MULTICAST':
            self.end_of_multicast = True
        return data
