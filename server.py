import os
import signal
import json

from flask import Flask, current_app
from flask import request

import server_init
import serialization
import read_dataset

# for debug
from icecream import ic
import time


app = Flask(__name__)

def get_dataset_metadata_form_name(name):
    return current_app.config['DATASET_METADATA'][name]

@app.route("/datasets/<dataset_name>", methods = ["GET"])
def get_dataset(dataset_name):
    dataset_metadata = get_dataset_metadata_form_name(dataset_name)

    optimized = dataset_metadata.get('optimized')
    if optimized:
        dataframe = read_dataset.read_dataset_optimized(dataset_metadata, row_input = request.json.get('row'), rows_input = request.json.get('rows'))
    else:
        dataframe = read_dataset.read_dataset(dataset_metadata)

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
    dataset_metadata = get_dataset_metadata_form_name(dataset_name)
    column_names = dataset_metadata['columns']
    response = json.dumps(column_names)
    return response

@app.route("/datasets/<dataset_name>/number_of_rows", methods = ["GET"])
def get_number_of_rows(dataset_name):
    dataset_metadata = get_dataset_metadata_form_name(dataset_name)

    number_of_rows = None
    if tmp := dataset_metadata.get('number_of_rows'):
        # if it is present in the metadata, use it
        number_of_rows = int(tmp)
    else:
        # else, calculate it by counting the lines

        paths = dataset_metadata['data_files']

        number_of_rows = 0
        for path in paths:
            with open(path, 'rt') as f:
                for line in f:
                    number_of_rows += 1
        if dataset_metadata.get('header_in_file') == True:
            number_of_rows = number_of_rows - 1*len(paths)

        # "cache" it
        dataset_metadata['number_of_rows'] = number_of_rows

    response = json.dumps(number_of_rows)
    return response

@app.route("/datasets")
def datasets():
    names = list(current_app.config["DATASET_METADATA"].keys())
    response = json.dumps(names)
    return response

# we use app.config to save the dataset metadata as a "global variable"
# we tried using a python's global variable, but it doesn't get updated by the SIGHUP handler, probably because of Flask's internal threads and processes
app.config['DATASET_METADATA'] = server_init.init()

def sighup_handler(signum, frame):
    with app.app_context():
        current_app.config["DATASET_METADATA"] = server_init.init()
        print('Reloading')

signal.signal(signal.SIGHUP, sighup_handler)

# TODO clean PID on exit
# Save the PID
# In debug mode two PIDs exist, so the second overwrites the first one
with open('/tmp/tfmpid', 'wt') as f:
    f.write(str(os.getpid()))

if __name__ == '__main__':
    app.run()
