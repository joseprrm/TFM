import unittest
import itertools

from icecream import ic

from client import Client

class TestWebsocket(unittest.TestCase):
    def setUp(self):
        self.client = Client.get_client("127.0.0.1", 5000, channel_method="websocket")
        self.ds = self.client.get_dataset('big_csv_int_1g_split')

    def tearDown(self):
        self.client.close()

    def test_websocket_basics(self):
        result = list(itertools.islice(self.ds.filter_columns('a'), 5))
        expect = [73, 65, 99, 55, 42]
        self.assertListEqual(result, expect)

class TestWebsocketPickle(TestWebsocket):
    def setUp(self):
        self.client = Client.get_client("127.0.0.1", 5000, channel_method="websocket", method="pickle")
        self.ds = self.client.get_dataset('big_csv_int_1g_split')

class TestWebsocketPickleCompressed(TestWebsocket):
    def setUp(self):
        self.client = Client.get_client("127.0.0.1", 5000, channel_method="websocket", method="pickle_compressed")
        self.ds = self.client.get_dataset('big_csv_int_1g_split')

class TestWebsocketJSON(TestWebsocket):
    def setUp(self):
        self.client = Client.get_client("127.0.0.1", 5000, channel_method="websocket", method="json")
        self.ds = self.client.get_dataset('big_csv_int_1g_split')
