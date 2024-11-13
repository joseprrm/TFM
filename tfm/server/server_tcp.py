import asyncio
import threading
import json

from icecream import ic
from flask import current_app

from server import process_query
import serialization

async def handle(reader, writer):
    while True:
        with app.app_context():
            data = await reader.read(4096)

            if data == b'':
                break
            
            query = json.loads(data)

            dataset = current_app.dataset[query['dataset_name']]
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

    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    print(f'Serving on {addrs}')

    async with server:
        await server.serve_forever()

app = None
def start(_app):
    global app
    app = _app
    # daemon=True makes the program stop and cleanup correctly with only one Ctrl-C 
    # otherwise, this part of the program only stops with two Ctrl-C 
    # the 
    thread = threading.Thread(target=asyncio.run, args=(tcp_run(),), daemon=True)
    thread.start()
