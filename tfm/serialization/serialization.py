import io
import base64
import json
import zlib
import pickle

from icecream import ic 

from abc import ABC, abstractmethod

import pandas

from utils import ProgrammingError

class Serializer(ABC):
    @abstractmethod
    def serialize(self, data):
        pass
    @abstractmethod
    def deserialize(self, data):
        pass

class PickleBase64Serializer(Serializer):
    def serialize(self, data):
        data_pickle = pickle.dumps(data)
        data_serialized = base64.b64encode(data_pickle)
        return data_serialized

    def deserialize(self, response):
        text = response.decode('utf-8')
        data_pickled = base64.b64decode(text)
        data = pickle.loads(data_pickled)
        return data

from numpyencoder import NumpyEncoder
class JSONSerializer(Serializer):
    def serialize(self, data):
        _json_str = ""
        match data:
            case pandas.DataFrame():
                _json_str = "dataframe," + data.to_json()
            case pandas.Series():
                _json_str = "series," + data.to_json()
            case _:
                _json_str = "builtin," + json.dumps(data, cls=NumpyEncoder)
        return _json_str

    def deserialize(self, response):
        #ic(response)
        #ic(type(response))
        data = None
        text = response.decode('utf-8')
        match text.split(",", 1):
            case ["builtin", _json]:
                data = json.loads(_json)
            case ["dataframe", _json]:
                data = pandas.read_json(io.StringIO(_json), typ = 'frame')
            case ["series", _json]:
                data = pandas.read_json(io.StringIO(_json), typ = 'series')
            case _:
                raise ProgrammingError
        return data

class JSONPlainSerializer(Serializer):
    def serialize(self, data):
        _json_str = ""
        match data:
            case pandas.DataFrame():
                _json_str = data.to_json()
            case pandas.Series():
                _json_str = data.to_json()
            case _:
                _json_str = json.dumps(data, cls=NumpyEncoder)
        return _json_str

    def deserialize(self, response):
        # no intented for use in the client library
        raise ProgrammingError

class PickleSerializer(Serializer):
    def serialize(self, data):
        data_serialized = pickle.dumps(data)
        return data_serialized

    def deserialize(self, response):
        # we take the raw bytes in the http payload
        data = pickle.loads(response)
        return data

class PickleCompressedSerializer(Serializer):
    def serialize(self, data):
        data_serialized = zlib.compress(pickle.dumps(data))
        return data_serialized

    def deserialize(self, response):
        # we take the raw bytes in the http payload
        data = pickle.loads(zlib.decompress(response))
        return data

mapping = {
    "pickle_base64": PickleBase64Serializer,
    "pickle": PickleSerializer,
    "pickle_compressed": PickleCompressedSerializer,
    "json": JSONSerializer,
    "json_plain": JSONPlainSerializer
}
