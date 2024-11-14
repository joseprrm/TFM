import asyncio

from icecream import ic
import time
from client.client import Client

queue = asyncio.Queue(maxsize=5000)
stop = asyncio.Event()

def blocking(data):
    time.sleep(0.00001)

async def print_queue():
    while not stop.is_set():
        await asyncio.sleep(1)
        ic(queue.qsize())

async def process_data():
    while True:
        data = await queue.get()
        if data == None:
            queue.task_done()
            break
        await asyncio.to_thread(blocking, data)
        queue.task_done()

    print("All items processed")
    stop.set()

async def fetch_data(dataset):
    async for e in dataset.filter_columns('C'):
        await queue.put(e)
    await queue.put(None)


async def main():
    _client = await Client.get_client_tcp_async("127.0.0.1", 5000, method="json")
    ds = _client.get_dataset('big_csv_int_1g_split')

    async with asyncio.TaskGroup() as tg:
        tg.create_task(fetch_data(ds))
        tg.create_task(process_data())
        tg.create_task(print_queue())
    await _client.closeasync()

asyncio.run(main())
