import sys
import os

import unittest
from icecream import ic
import pandas as pd

from client import Client

class TestDataset(unittest.TestCase):
    def setUp(self):
        _client = Client.get_client("127.0.0.1", 5000, method="json")
        self.ds_multiple_files_no_config = _client.get_dataset('test_multiple_files_no_config')
        self.ds_multiple_files_config = _client.get_dataset('test_multiple_files_config')
        self.ds_multiple_files_config_glob = _client.get_dataset('test_multiple_files_config_glob')
        self.ds_multiple_files_config_glob_in_list_one = _client.get_dataset('test_multiple_files_config_glob_in_list_one')
        self.ds_multiple_files_config_glob_in_list_multiple = _client.get_dataset('test_multiple_files_config_glob_in_list_multiple')

    def test_multiple_files_no_config(self):
        self.assertEqual(len([x for x in self.ds_multiple_files_no_config]), 9)

    def test_multiple_files_config(self):
        self.assertEqual(len([x for x in self.ds_multiple_files_config]), 9)

    def test_multiple_files_config_glob(self):
        self.assertEqual(len([x for x in self.ds_multiple_files_config_glob]), 9)

    def test_multiple_files_config_glob_in_list_one(self):
        self.assertEqual(len([x for x in self.ds_multiple_files_config_glob_in_list_one]), 9)

    def test_multiple_files_config_glob_in_list_multiple(self):
        self.assertEqual(len([x for x in self.ds_multiple_files_config_glob_in_list_multiple]), 18)
        _list1 = ["setosa"] * 3 + ["virginica"] * 3 + ["versicolor"] * 3 + ["versicolor", "setosa", "setosa"]+ [ "setosa", "versicolor", "setosa"]+ ["setosa", "setosa", "versicolor"]
        _list2 = [x.species for x in self.ds_multiple_files_config_glob_in_list_multiple]
        for e1, e2 in zip(_list1, _list2):
            self.assertEqual(e1, e2)

if __name__ == '__main__':
    unittest.main()
