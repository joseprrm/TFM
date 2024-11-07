import socketserver
import threading
import json

from icecream import ic
from flask import current_app

from server import app, process_query
import serialization

class RequestHandler(socketserver.BaseRequestHandler):
    def handle(self):
        with app.app_context():
            self.data = self.request.recv(10000)
            
            query = json.loads(self.data)

            dataset = current_app.dataset[query['dataset_name']]
            dataframe = process_query(dataset, query)
            data_serialized = serialization.mapping[query['method']]().serialize(dataframe)

            if isinstance(data_serialized, str):
                data_serialized = bytes(data_serialized, 'utf-8')

            self.request.sendall(data_serialized)

server = None
def start():
    global server
    try:
        address = ('localhost', 8000)  # let the kernel assign a port
        server = socketserver.TCPServer(address, RequestHandler)

        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
    except OSError:
        pass

def shutdown():
    global server
    ic("Shutdown tcp")
    server.shutdown()
    server.socket.close()
