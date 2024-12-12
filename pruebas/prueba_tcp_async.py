import asyncio

from icecream import ic
import time
from client.client import Client

queue = asyncio.Queue(maxsize=5000)
stop = asyncio.Event()

def blocking(data):
    time.sleep(0.001)

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
    async for e in dataset.filter_columns('col1').row_iterator_async(0, 10000):
        await queue.put(e)
    await queue.put(None)

async def main():
    _client = await Client.get_client_tcp_async("127.0.0.1", 5000, method="pickle")
    dataset = _client.get_dataset('big_partitions')

    time_start = time.perf_counter()
    async with asyncio.TaskGroup() as tg:
        tg.create_task(fetch_data(dataset))
        tg.create_task(process_data())
        tg.create_task(print_queue())
    await _client.closeasync()
    time_end = time.perf_counter()
    time_seconds = (time_end - time_start)
    print(time_seconds)

asyncio.run(main())
