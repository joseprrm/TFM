import os
import signal
import json

from flask import Flask, current_app
from flask import request

import server_init
import serialization

# for debug
from icecream import ic
import time


app = Flask(__name__)

def get_dataset_metadata_form_name(name):
    return current_app.dataset_metadatas[name]

@app.route("/datasets/<dataset_name>", methods = ["GET"])
def get_dataset(dataset_name):
    dataset = current_app.dataset[dataset_name]

    optimized = dataset.optimized
    if optimized:
        dataframe = dataset.read_optimized(row_input = request.json.get('row'), rows_input = request.json.get('rows'))
    else:
        dataframe = dataset.read()

    if request.json.get('random') == True:
        if request.json.get('number_of_samples'):
            dataframe = dataframe.sample(request.json.get('number_of_samples'))
        else:
            dataframe = dataframe.sample(1)
    if request.json.get('column'):
        dataframe = dataframe[request.json['column']]
    if request.json.get('columns'):
        dataframe = dataframe[request.json['columns']]
    if request.json.get('rows') :
        step = None 
        if request.json.get('step'):
            step = request.json.get('step')
        else:
            step = 1
        # -1 to make it not include the last number 
        dataframe = dataframe.loc[request.json["rows"][0]:(request.json["rows"][1] - 1):step]
        # Reindex to give a Series/DataFrame with indexes starting from 0
        dataframe = dataframe.reset_index(drop=True)

    if request.json.get('row') is not None:
        dataframe = dataframe.loc[request.json["row"]]

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
            with open(path, 'rt') as f:
                for line in f:
                    number_of_rows += 1
        if dataset.header_in_file == True:
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

if __name__ == '__main__':
    app.run()
