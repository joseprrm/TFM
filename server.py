import os
import signal
import csv
import pickle
import json
import base64
import pandas
import pandas as pd
import yaml
from icecream import ic
import glob
import time

from server_init import init
from utils import is_number


from flask import Flask, current_app
from flask import request

app = Flask(__name__)


@app.route("/datasets/<dataset_name>", methods = ["GET"])
def get_dataset(dataset_name):
    data_dataframe = None
    optimized = current_app.config['DATASET_METADATA'][dataset_name].get('optimized')

    if optimized:
        if request.json.get('row') is not None:
            data_dataframe = read_dataset_optimized(dataset_name, row_input = request.json.get('row'))
        elif request.json.get('rows') is not None:
            data_dataframe = read_dataset_optimized(dataset_name, rows_input = request.json.get('rows'))
    else:
        data_dataframe = read_dataset(dataset_name)

    # TODO delete if
    if request.data:
        if request.json.get('random') == True:
            if request.json.get('number_of_samples'):
                data_dataframe = data_dataframe.sample(request.json.get('number_of_samples'))
            else:
                data_dataframe = data_dataframe.sample(1)

        if request.json.get('column'):
            data_dataframe = data_dataframe[request.json['column']]
        if request.json.get('columns'):
            data_dataframe = data_dataframe[request.json['columns']]
        if request.json.get('rows') :
            step = None 
            if request.json.get('step'):
                step = request.json.get('step')
            else:
                step = 1
            data_dataframe = data_dataframe.iloc[request.json["rows"][0]:request.json["rows"][1]:step]
            # Reindex to give a Series/DataFrame with indexes starting from 0
            data_dataframe = data_dataframe.reset_index(drop=True)

        #if optimized:
        #    data_dataframe.index = range(optimized_start, optimized_end)


        if request.json.get('row') is not None:
            data_dataframe = data_dataframe.loc[request.json["row"]]

    data_serialized = None

    match request.json.get('method'):
        case 'pickle_base64':
            data_serialized = serialize_pickle_base64(data_dataframe)
        case 'pickle':
            data_serialized = serialize_pickle(data_dataframe)
        case 'pickle_compressed':
            data_serialized = serialize_pickle_compressed(data_dataframe)
        case 'json':
            data_serialized = serialize_json(data_dataframe)
        case _:
            raise Exception("PROGRAMMING ERROR")
    return data_serialized

def serialize_pickle_base64(data):
    data_pickle = pickle.dumps(data)
    data_serialized = base64.b64encode(data_pickle)
    return data_serialized

def serialize_pickle(data):
    data_serialized = pickle.dumps(data)
    return data_serialized

import zlib
def serialize_pickle_compressed(data):
    data_serialized = zlib.compress(pickle.dumps(data))
    return data_serialized

def serialize_json(data):
    _json_str = ""
    match data:
        case pandas.DataFrame():
            _json_str = "dataframe," + data.to_json()
        case pandas.Series():
            _json_str = "series," + data.to_json()
        case _:
            _json_str = "builtin," + json.dumps(data)
    return _json_str


def read_one_csv(path, dataset_info):
    dataframe = None
    if dataset_info.get('header_in_file') == False:
        dataframe = pandas.read_csv(path, header=None)
    else:
        dataframe = pandas.read_csv(path)
    return dataframe

def first_true_element(l):
    for i, boolean in enumerate(l):
        if boolean:
            return i
    return None

def read_dataset_optimized(dataset_name, row_input = None, rows_input = None):
    # first we search in the dataset configurations
    dataset_info = current_app.config['DATASET_METADATA'][dataset_name]
    paths = dataset_info['data_files']
    rows = dataset_info['rows']

    if row_input is not None:
        rows_input = [row_input, row_input]

    first_row, last_row = rows_input

    # determine the partition
    rows = [list(x.keys())[0] for x in rows]

    index_first_row = first_true_element([first_row < i for i in rows])
    index_last_row = first_true_element([last_row < i for i in rows])

    ic(index_first_row)
    ic(index_last_row)

    dataframe = None
    if index_first_row == index_last_row:
        index = index_first_row
        dataframe = read_one_csv(paths[index], dataset_info)
    else:
        for index in range(index_first_row, index_last_row + 1):
            df1 = dataframe
            df2 = read_one_csv(paths[index], dataset_info)
            dataframe = pandas.concat([df1, df2], ignore_index=True) # ignore index to reindex

    dataframe.columns = dataset_info['columns']

    start = 0 if index_first_row == 0 else rows[index_first_row - 1]
    end = rows[index_last_row]

    dataframe.index = range(start, end)
    return dataframe

def read_dataset(dataset_name):
    # first we search in the dataset configurations
    dataset_info = current_app.config['DATASET_METADATA'][dataset_name]
    paths = dataset_info['data_files']

    dataframe = None
    for path in paths[0:]:
        df1 = dataframe
        df2 = read_one_csv(path, dataset_info)
        dataframe = pandas.concat([df1, df2], ignore_index=True) # ignore index to reindex

    dataframe.columns = dataset_info['columns']
    return dataframe

@app.route("/datasets/<dataset_name>/column_names", methods = ["GET"])
def get_column_names(dataset_name):
    column_names = current_app.config["DATASET_METADATA"][dataset_name]['columns']
    response = json.dumps(column_names)
    return response

@app.route("/datasets/<dataset_name>/number_of_rows", methods = ["GET"])
def get_number_of_rows(dataset_name):
    dataset_info = current_app.config['DATASET_METADATA'][dataset_name]

    number_of_rows = None
    if tmp := dataset_info.get('number_of_rows'):
        # if it is present in the metadata, use it
        number_of_rows = int(tmp)
    else:
        # else, calculate it by counting the lines

        paths = dataset_info['data_files']

        number_of_rows = 0
        for path in paths:
            with open(path, 'rt') as f:
                for line in f:
                    number_of_rows += 1
        if dataset_info.get('header_in_file') == True:
            number_of_rows = number_of_rows - 1*len(paths)

        # "cache" it
        dataset_info['number_of_rows'] = number_of_rows

    response = json.dumps(number_of_rows)
    return response

@app.route("/datasets")
def datasets():
    names = list(current_app.config["DATASET_METADATA"].keys())
    response = json.dumps(names)
    return response

@app.route("/prova")
def prova():
    print(current_app.config["DATASET_METADATA"].get("kk"))
    return json.dumps("OK")


# we use app.config to save the dataset metadata as a "global variable"
# we tried using a python's global variable, but it doesn't get updated by the SIGHUP handler, probably because of Flask's internal threads and processes
with app.app_context():
    current_app.config['DATASET_METADATA'] = init()

# signal handler for SIGHUP
def sighup_handler(signum, frame):
    with app.app_context():
        current_app.config["DATASET_METADATA"] = init()
        print(current_app.config["DATASET_METADATA"].get("kk"))
        print('Reloading')

signal.signal(signal.SIGHUP, sighup_handler)

# TODO clean PID on exit
# Save the PID
# In debug mode two PIDs exist, so the second overwrites the first one
with open('/tmp/tfmpid', 'wt') as f:
    f.write(str(os.getpid()))

if __name__ == '__main__':
    app.run()
