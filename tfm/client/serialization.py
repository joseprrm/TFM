import io
import base64
import json
import zlib
import pickle

from abc import ABC, abstractmethod

import pandas

from utils import ProgrammingError

class Serializer(ABC):
    @abstractmethod
    def deserialize(self, data):
        pass

class PickleBase64Serializer(Serializer):
    def deserialize(self, response):
        text = response.decode('utf-8')
        data_pickled = base64.b64decode(text)
        data = pickle.loads(data_pickled)
        return data

class JSONSerializer(Serializer):
    def deserialize(self, response):
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

class PickleSerializer(Serializer):
    def deserialize(self, response):
        # we take the raw bytes in the http payload
        data = pickle.loads(response)
        return data

class PickleCompressedSerializer(Serializer):
    def deserialize(self, response):
        # we take the raw bytes in the http payload
        data = pickle.loads(zlib.decompress(response))
        return data

mapping = {
    "pickle_base64": PickleBase64Serializer,
    "pickle": PickleSerializer,
    "pickle_compressed": PickleCompressedSerializer,
    "json": JSONSerializer
}
