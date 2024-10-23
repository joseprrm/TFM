import json
import pickle
import base64
import zlib
import pandas

def serialize(data, method):
    match method:
        case 'pickle_base64':
            data_serialized = _serialize_pickle_base64(data)
        case 'pickle':
            data_serialized = _serialize_pickle(data)
        case 'pickle_compressed':
            data_serialized = _serialize_pickle_compressed(data)
        case 'json':
            data_serialized = _serialize_json(data)
        case _:
            raise Exception("PROGRAMMING ERROR")
    return data_serialized

def _serialize_pickle_base64(data):
    data_pickle = pickle.dumps(data)
    data_serialized = base64.b64encode(data_pickle)
    return data_serialized

def _serialize_pickle(data):
    data_serialized = pickle.dumps(data)
    return data_serialized

def _serialize_pickle_compressed(data):
    data_serialized = zlib.compress(pickle.dumps(data))
    return data_serialized

def _serialize_json(data):
    _json_str = ""
    match data:
        case pandas.DataFrame():
            _json_str = "dataframe," + data.to_json()
        case pandas.Series():
            _json_str = "series," + data.to_json()
        case _:
            _json_str = "builtin," + json.dumps(data)
    return _json_str
