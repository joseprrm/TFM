import threading
import asyncio
import json
import serialization

from flask import current_app
from server import app, foo

from websockets.asyncio.server import serve

async def echo(websocket):
    async for message in websocket:
        with app.app_context():
            query = json.loads(message)
            dataset = current_app.dataset['big_csv_int_1g_split']
            #ic(query)
            #ic(dataset)
            dataframe = foo(dataset, query)
            data_serialized = serialization.serialize(dataframe, 'pickle_base64')
            #ic(dataframe)
            #data_serialized = serialization.serialize(0, 'pickle_base64')
            await websocket.send(data_serialized)

async def websocket_run():
    try:
        async with serve(echo, "localhost", 30004):
            await asyncio.get_running_loop().create_future()  # run forever
    except OSError:
        pass

def start():
    thread = threading.Thread(target=asyncio.run, args=(websocket_run(),))
    thread.start()
