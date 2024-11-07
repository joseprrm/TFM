import threading
import asyncio
import json

from icecream import ic
from flask import current_app
from websockets.asyncio.server import serve

from server import app, process_query
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
        async with serve(callback, "localhost", 30004):
            await asyncio.get_running_loop().create_future()  # run forever
    except OSError:
        pass

def start():
    thread = threading.Thread(target=asyncio.run, args=(websocket_run(),))
    thread.start()
