import threading
import asyncio
import json

from icecream import ic
from flask import current_app
import websockets.asyncio.server

from server import process_query
import serialization

async def callback(websocket):
    async for message in websocket:
        with app.app_context():
            query = json.loads(message)

            dataset = current_app.dataset[query['dataset_name']]
            dataframe = process_query(dataset, query)
            data_serialized = serialization.mapping[query['method']]().serialize(dataframe)

            await websocket.send(data_serialized)

async def websocket_run():
    try:
        print('Starting websocket')
        async with websockets.asyncio.server.serve(callback, "localhost", 30004):
            await asyncio.get_running_loop().create_future()  # run forever
    except OSError:
        print('Websocket already started')

app = None
def start(_app):
    global app
    app = _app
    # daemon=True makes the program stop and cleanup correctly with only one Ctrl-C 
    # otherwise, this part of the program only stops with two Ctrl-C 
    # the 
    thread = threading.Thread(target=asyncio.run, args=(websocket_run(),), daemon=True)
    thread.start()

def shutdown():
    ic('Shutdown websocket')
