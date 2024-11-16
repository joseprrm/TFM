import asyncio
import threading
import json

from icecream import ic

from process_query import process_query
import serialization

async def handle(reader, writer):
    while True:
        data = await reader.read(4096)

        if data == b'':
            break
        
        query = json.loads(data)

        dataset = datasets[query['dataset_name']]

        dataframe = process_query(dataset, query)
        data_serialized = serialization.mapping[query['method']]().serialize(dataframe)

        if isinstance(data_serialized, str):
            data_serialized = bytes(data_serialized, 'utf-8')

        writer.write(data_serialized)
        await writer.drain()


async def tcp_run():
    print('Starting tcp server')
    server = await asyncio.start_server(
        handle, '127.0.0.1', 8000)

    async with server:
        await server.serve_forever()

datasets = None
def start(_datasets):
    global datasets
    datasets = _datasets
    # daemon=True makes the program stop and cleanup correctly with only one Ctrl-C 
    # otherwise, this part of the program only stops with two Ctrl-C 
    # the 
    thread = threading.Thread(target=asyncio.run, args=(tcp_run(),), daemon=True)
    thread.start()
