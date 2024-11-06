import threading
import asyncio
import json

from flask import current_app
from websockets.asyncio.server import serve

from server import app, foo
import serialization

async def echo(websocket):
    async for message in websocket:
        with app.app_context():
            query = json.loads(message)
            dataset = current_app.dataset['big_csv_int_1g_split']
            dataframe = foo(dataset, query)
            data_serialized = serialization.serialize(dataframe, 'pickle_base64')
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
