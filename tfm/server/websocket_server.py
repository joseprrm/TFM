import threading
import asyncio
import json

from icecream import ic
import websockets.asyncio.server

from process_query import process_query
import serialization

async def callback(websocket):
    async for message in websocket:
        query = json.loads(message)

        dataset = datasets[query['dataset_name']]
        dataframe = process_query(dataset, query)
        data_serialized = serialization.mapping[query['method']]().serialize(dataframe)

        await websocket.send(data_serialized)

async def websocket_run():
    print('Starting websocket server')
    async with websockets.asyncio.server.serve(callback, "localhost", 30004):
        await asyncio.get_running_loop().create_future()  # run forever

datasets = None
def start(_datasets):
    # daemon=True makes the program stop and cleanup correctly with only one Ctrl-C 
    # otherwise, this part of the program only stops with two Ctrl-C 
    global datasets
    datasets = _datasets
    thread = threading.Thread(target=asyncio.run, args=(websocket_run(),), daemon=True)
    thread.start()
