import os
import signal
import time

# for debug
from icecream import ic

import initialization
import clear_cache
import flask_server
import fastapi_server
import tcp_server
import websocket_server

# global variable that holds the Dataset objects
datasets = None

# TODO clean PID on exit
def write_pid_file():
    with open('/tmp/tfmpid', 'wt') as f:
        f.write(str(os.getpid()))

def sighup_handler(signum, frame):
    print('Reloading')
    global datasets
    _datasets = initialization.initialize_dataset_dict()
    # clear and update so we keep using the same dictionary. This is important, because if we directly set the global variable instead, then the other threads will keep using the old reference.
    datasets.clear()
    datasets.update(_datasets)

def main():
    try:
        global datasets
        # only time this variable should be defined
        datasets = initialization.initialize_dataset_dict()

        clear_cache.start(datasets)
        flask_server.start(datasets)
        #fastapi_server.start(datasets)
        websocket_server.start(datasets)
        tcp_server.start(datasets)

        # wait for the other threads to run wasting small amount of CPU 
        while True:
            time.sleep(999999)

    except KeyboardInterrupt as e:
        print('Program stopped by Ctrl-C')
    except Exception as e:
        print(e)
    finally:
        print('Exiting')

if __name__ == '__main__':
    write_pid_file()
    signal.signal(signal.SIGHUP, sighup_handler)
    main()
