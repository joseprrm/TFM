import io
import base64
import json
import zlib
import pickle

import pandas

from icecream import ic

from utils import ProgrammingError

class PickleBase64Serializer():
    def deserialize(self, response):
        text = response.decode('utf-8')
        data_pickled = base64.b64decode(text)
        data = pickle.loads(data_pickled)
        return data

class JSONSerializer():
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

class PickleSerializer():
    def deserialize(self, response):
        # we take the raw bytes in the http payload
        data = pickle.loads(response)
        return data

class PickleCompressedSerializer():
    def deserialize(self, response):
        # we take the raw bytes in the http payload
        data = pickle.loads(zlib.decompress(response))
        return data
