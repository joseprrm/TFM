import os
import json
import threading

# for debug
from icecream import ic

from flask import Flask, current_app
from flask import request
import waitress

import serialization
import multicast

from process_query import process_query

app = Flask(__name__)
datasets = None

@app.route("/datasets/<dataset_name>", methods = ["GET"])
def get_dataset(dataset_name):
    dataset = current_app.datasets[dataset_name]

    query = request.json
    dataframe = process_query(dataset, query)

    data_serialized = serialization.mapping[query['method']]().serialize(dataframe)
    return data_serialized

@app.route("/datasets/<dataset_name>/column_names", methods = ["GET"])
def get_column_names(dataset_name):

    dataset = current_app.datasets[dataset_name]
    column_names = dataset.columns
    response = json.dumps(column_names)

    return response

@app.route("/datasets/<dataset_name>/number_of_rows", methods = ["GET"])
def get_number_of_rows(dataset_name):
    ic('get_number_of_rows()')
    dataset = current_app.datasets[dataset_name]

    if tmp := dataset.number_of_rows:
        # if it is present in the metadata, use it
        number_of_rows = int(tmp)
    else:
        # else, calculate it by counting the lines

        number_of_rows = 0
        for path in dataset.paths:
            with open(path, 'rb') as file:
                number_of_rows += sum(1 for _ in file)
        if dataset.header_in_file is True:
            number_of_rows = number_of_rows - 1*len(dataset.paths)

        # "cache" it
        dataset.number_of_rows = number_of_rows

    response = json.dumps(dataset.number_of_rows)
    return response

@app.route("/datasets")
def datasets():
    names = list(current_app.datasets.keys())
    response = json.dumps(names)
    return response

@app.route("/datasets/<dataset_name>/multicast", methods = ["GET"])
def get_dataset_multicast(dataset_name):
    ic(f'/datasets/{dataset_name}/multicast')
    dataset = current_app.datasets[dataset_name]

    query = request.json
    ic(query)
    dataframe = process_query(dataset, query)

    data_serialized = serialization.mapping[query['method']]().serialize(dataframe)
    multicast.send(query['ip'], query['port'], data_serialized)
    return ""

@app.route("/multicast_end", methods = ["GET"])
def multicast_end():
    query = request.json
    data_serialized = serialization.mapping[query['method']]().serialize("END_OF_MULTICAST")
    multicast.send(query['ip'], query['port'], data_serialized)
    return ""

def flask_run():
    print('Starting flask server')
    waitress.serve(app, port=5000)

def start(_datasets):
    app.datasets = _datasets
    thread = threading.Thread(target=flask_run, daemon=True)
    thread.start()
