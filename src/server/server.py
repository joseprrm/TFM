import os
import signal
import json
import threading

# for debug
import time
from icecream import ic

import websockets
from flask import Flask, current_app
from flask import request

import server_init
import serialization


app = Flask(__name__)

#import logging
#log = logging.getLogger('werkzeug')
#log.setLevel(logging.ERROR)

def foo(dataset, query):
    optimized = dataset.optimized
    if optimized:
        dataframe = dataset.read_optimized(row_input = query.get('row'),
                                           rows_input = query.get('rows'))
    else:
        dataframe = dataset.read()

    if query.get('random') is True:
        if query.get('number_of_samples'):
            dataframe = dataframe.sample(query.get('number_of_samples'))
        else:
            dataframe = dataframe.sample(1)
    if query.get('column'):
        dataframe = dataframe[query['column']]
    if query.get('columns'):
        dataframe = dataframe[query['columns']]
    if query.get('rows') :
        step = None
        if query.get('step'):
            step = query.get('step')
        else:
            step = 1
        # -1 to make it not include the last number
        dataframe = dataframe.loc[query["rows"][0]:(query["rows"][1] - 1):step]
        # Reindex to give a Series/DataFrame with indexes starting from 0
        dataframe = dataframe.reset_index(drop=True)

    if query.get('row') is not None:
        dataframe = dataframe.loc[query["row"]]

    return dataframe


#    data_serialized = serialization.serialize(0, request.json.get('method'))
#    return data_serialized
@app.route("/datasets/<dataset_name>", methods = ["GET"])
def get_dataset(dataset_name):
    dataset = current_app.dataset[dataset_name]

    query = request.json
    #ic(query)
    dataframe = foo(dataset, query)

    data_serialized = serialization.serialize(dataframe, request.json.get('method'))
    return data_serialized

@app.route("/datasets/<dataset_name>/column_names", methods = ["GET"])
def get_column_names(dataset_name):
    dataset = current_app.dataset[dataset_name]
    column_names = dataset.columns
    response = json.dumps(column_names)
    return response

@app.route("/datasets/<dataset_name>/number_of_rows", methods = ["GET"])
def get_number_of_rows(dataset_name):
    dataset = current_app.dataset[dataset_name]

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
    names = list(current_app.dataset.keys())
    response = json.dumps(names)
    return response

with app.app_context():
    current_app.dataset = server_init.init()

def sighup_handler(signum, frame):
    with app.app_context():
        current_app.dataset_metadatas = server_init.init()
        print('Reloading')

signal.signal(signal.SIGHUP, sighup_handler)

# TODO clean PID on exit
# Save the PID
# In debug mode two PIDs exist, so the second overwrites the first one
with open('/tmp/tfmpid', 'wt') as f:
    f.write(str(os.getpid()))

import server_websocket
server_websocket.start()

if __name__ == '__main__':
    app.run()
