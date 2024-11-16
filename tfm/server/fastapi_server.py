import threading
import json

from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
import uvicorn
# For debug
from icecream import ic

import serialization
from process_query import process_query


# FastAPI setup
app = FastAPI()


# Pydantic model for the request body
class Query(BaseModel):
    method: str
    # Add other fields here if needed based on your query structure

@app.get("/datasets/{dataset_name}")
async def get_dataset(dataset_name: str, request: Request):
    dataset = datasets[dataset_name]
    query = await request.json()  # Parse JSON body of the request
    dataframe = process_query(dataset, query)

    data_serialized = serialization.mapping[query['method']]().serialize(dataframe)
    return PlainTextResponse(content=data_serialized)

@app.get("/datasets/{dataset_name}/column_names")
async def get_column_names(dataset_name: str):
    dataset = datasets[dataset_name]
    column_names = dataset.columns
    return column_names

@app.get("/datasets/{dataset_name}/number_of_rows")
async def get_number_of_rows(dataset_name: str):
    dataset = datasets[dataset_name]

    if tmp := dataset.number_of_rows:
        # If it's present in the metadata, use it
        number_of_rows = int(tmp)
    else:
        # Else, calculate it by counting the lines
        number_of_rows = 0
        for path in dataset.paths:
            with open(path, 'rb') as file:
                number_of_rows += sum(1 for _ in file)
        if dataset.header_in_file is True:
            number_of_rows = number_of_rows - 1 * len(dataset.paths)

        # "Cache" it
        dataset.number_of_rows = number_of_rows

    return dataset.number_of_rows

@app.get("/datasets")
async def get_datasets():
    names = list(datasets.keys())
    return names

def flask_run():
    print('Starting FastAPI server')
    uvicorn.run(app, port=5000)

datasets = None
def start(_datasets):
    global datasets
    datasets = _datasets
    thread = threading.Thread(target=flask_run, daemon=True)
    thread.start()

