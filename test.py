import unittest
from icecream import ic
import pandas as pd

from client import Client

class TestClient(unittest.TestCase):
    def setUp(self):
        self.client = Client.get_client("127.0.0.1", 5000)

    def test_list_datasets(self):
        dataset_names = self.client.list_datasets()
        self.assertIn("iris_test", dataset_names)
        self.assertIn("iris_test_small", dataset_names)
        self.assertIn("iris_test_col", dataset_names)

    def test_number_of_rows(self):
        number_of_rows = self.client.number_of_rows('iris_test')
        self.assertEqual(number_of_rows, 15)

        number_of_rows = self.client.number_of_rows('iris_test_small')
        self.assertEqual(number_of_rows, 3)

        number_of_rows = self.client.number_of_rows('iris_test_col')
        self.assertEqual(number_of_rows, 3)

    def test_column_names(self):
        column_names = self.client.column_names('iris_test')
        self.assertIn("species", column_names)
        self.assertIn("petal_length", column_names)
        self.assertIn("petal_width", column_names)
        self.assertIn("sepal_length", column_names)
        self.assertIn("sepal_width", column_names)

    def test_column_names_no_header(self):
        column_names = self.client.column_names('iris_test_no_header')
        self.assertIn("X0", column_names)
        self.assertIn("X1", column_names)
        self.assertIn("X2", column_names)
        self.assertIn("X3", column_names)
        self.assertIn("X4", column_names)

    def test_column_names_config(self):
        column_names = self.client.column_names('iris_test_col')
        self.assertIn("col1", column_names)
        self.assertIn("col2", column_names)
        self.assertIn("col3", column_names)
        self.assertIn("col4", column_names)
        self.assertIn("species", column_names)

    def test_column_names_config_2(self):
        column_names = self.client.column_names('test_all_strings')
        self.assertIn("a", column_names)
        self.assertIn("d", column_names)
        self.assertIn("g", column_names)
        self.assertIn("j", column_names)
        self.assertIn("m", column_names)

        column_names = self.client.column_names('test_all_strings_no_header')
        self.assertIn("X0", column_names)
        self.assertIn("X1", column_names)
        self.assertIn("X2", column_names)
        self.assertIn("X3", column_names)
        self.assertIn("X4", column_names)

class TestClientJSON(TestClient):
    def setUp(self):
        self.client = Client.get_client("127.0.0.1", 5000, method="json")

class TestDataset(unittest.TestCase):
    def setUp(self):
        self._client = Client.get_client("127.0.0.1", 5000)
        self.setup_datasets()

    def setup_datasets(self):
        # It has 15 rows
        self.ds = self._client.get_dataset('iris_test')
        # It has 3 rows
        self.ds_small = self._client.get_dataset('iris_test_small')
        # Different columns
        self.ds_columns_col = self._client.get_dataset('iris_test_col')
        # No header
        self.ds_columns_X = self._client.get_dataset('iris_test_no_header')

    def test_column_1(self):
        for e1, e2 in zip(list(self.ds_columns_col.columns), ["col1", "col2", "col3", "col4", "species"]):
            self.assertEqual(e1, e2)

        for e1, e2 in zip(list(self.ds_columns_X.columns), ["X0", "X1", "X2", "X3", "X4"]):
            self.assertEqual(e1, e2)

        for e1, e2 in zip(list(self.ds.columns), ["sepal_length", "sepal_width", "petal_length", "petal_width", "species"]):
            self.assertEqual(e1, e2)

        for e1, e2 in zip(list(self.ds_columns_col.iloc[0:2].columns), ["col1", "col2", "col3", "col4", "species"]):
            self.assertEqual(e1, e2)

        for e1, e2 in zip(list(self.ds_columns_X.iloc[0:2].columns), ["X0", "X1", "X2", "X3", "X4"]):
            self.assertEqual(e1, e2)

        for e1, e2 in zip(list(self.ds.iloc[0:2].columns), ["sepal_length", "sepal_width", "petal_length", "petal_width", "species"]):
            self.assertEqual(e1, e2)

    def test_iteration(self):
        self.assertEqual(len([x for x in self.ds_small]), 3)

        for i, e in enumerate(self.ds_small):
            if i == 0:
                self.assertEqual(e.species, "virginica")
                self.assertEqual(e.sepal_length, 5.9)
            elif i == 1:
                self.assertEqual(e.species, "setosa")
                self.assertEqual(e.sepal_length, 4.6)
            elif i == 2:
                self.assertEqual(e.species, "versicolor")
                self.assertEqual(e.sepal_length, 5.6)
            else:
                self.fail()

    def test_getitem_str(self):
        data = self.ds["species"]
        self.assertIsInstance(data, pd.core.series.Series)
        self.assertEqual(data[0], "virginica")
        self.assertEqual(data[1], "setosa")
        with self.assertRaises(KeyError):
            data[20]

    def test_getitem_list_of_str(self):
        data = self.ds[["species", "sepal_length"]]
        self.assertIsInstance(data, pd.core.frame.DataFrame)
        data.species
        data.sepal_length
        with self.assertRaises(AttributeError):
            data.sepal_width

    def test_getitem_range_without_step(self):
        data = self.ds[range(0, 3)]
        self.assertIsInstance(data, pd.core.frame.DataFrame)
        self.assertEqual(len(data), 3)

    def test_getitem_range_with_step(self):
        data = self.ds[range(0, 10, 2)]
        self.assertIsInstance(data, pd.core.frame.DataFrame)
        self.assertEqual(len(data), 10//2)
        self.assertEqual(data.sepal_length[0], 5.9)
        self.assertEqual(data.petal_width[4], 1.3)

    def test_getitem_int(self):
        data = self.ds[9]
        self.assertIsInstance(data, pd.core.series.Series)
        self.assertEqual(data.species, "virginica")
        self.assertEqual(data["sepal_length"], 6.5)

    def test_getitem_int_str(self):
        data = self.ds[9, "species"]
        self.assertIsInstance(data, str)
        data = self.ds[9, "sepal_length"]
        self.assertIsInstance(data, float)

    def test_getitem_range_str(self):
        data = self.ds[range(2,6, 2), "species"]
        self.assertIsInstance(data, pd.core.series.Series)
        self.assertEqual(len(data), (6-2)//2)
        self.assertEqual(data[0], "versicolor")

    def test_getitem_int_list_of_str(self):
        data = self.ds[9, ["species", "sepal_length"]]
        self.assertIsInstance(data, pd.core.series.Series)
        self.assertEqual(data.species, "virginica")
        self.assertEqual(data.sepal_length, 6.5)
        with self.assertRaises(AttributeError):
            data.sepal_width

    def test_getitem_range_list_of_str(self):
        data = self.ds[range(3, 5), ["species", "sepal_length"]]
        self.assertIsInstance(data, pd.core.frame.DataFrame)
        for e1, e2 in zip(data.species, ["setosa", "virginica"]):
            self.assertEqual(e1, e2)
        with self.assertRaises(AttributeError):
            data.sepal_width

    def test_getitem_range_with_step_list_of_str(self):
        data = self.ds[range(1, 10, 3), ["species", "sepal_length"]]
        self.assertIsInstance(data, pd.core.frame.DataFrame)
        for e1, e2 in zip(data.species, ["setosa", "virginica", "setosa"]):
            self.assertEqual(e1, e2)
        with self.assertRaises(AttributeError):
            data.sepal_width


    def test_iloc(self):
        self.assertEqual(self.ds.iloc[0].species, "virginica")
        self.assertEqual(self.ds.iloc[1].species, "setosa")
        for e1, e2 in zip(self.ds.iloc[2:5].species, ["versicolor", "setosa", "virginica"]):
            self.assertEqual(e1, e2)

        self.assertTrue(self.ds.iloc[0:10].equals(self.ds.iloc[0:10]))

    def test_use_columns(self):
        self.ds.use_columns('species')
        self.assertEqual(self.ds.iloc[0], "virginica")
        self.assertEqual(self.ds.iloc[1], "setosa")
        for e1, e2 in zip(self.ds.iloc[2:5], ["versicolor", "setosa", "virginica"]):
            self.assertEqual(e1, e2)

        self.ds.use_columns(['species', 'sepal_width'])
        self.assertEqual(self.ds.iloc[0].species, "virginica")
        self.assertEqual(self.ds.iloc[0].sepal_width, 3.0)
        with self.assertRaises(AttributeError):
            self.ds.iloc[0].sepal_length

        # After the reset we can access everything
        self.ds.use_columns_reset()
        self.assertEqual(self.ds.iloc[14].species, "versicolor")
        self.assertEqual(self.ds.iloc[14].sepal_length, 5.7)
        self.assertEqual(self.ds.iloc[14].sepal_width, 3.0)
        self.assertEqual(self.ds.iloc[14].petal_length, 4.2)
        self.assertEqual(self.ds.iloc[14].petal_width, 1.2)

    def test_row_iteration(self):
        for e1, e2 in zip([x.species for x in self.ds.row_iterator(0, 3)], ["virginica", "setosa", "versicolor"]):
            self.assertEqual(e1, e2)

        data = [x for x in self.ds.row_iterator(0, 3)]

    def test_row_iteration_use_columns(self):
        self.ds.use_columns(['species', 'petal_length'])
        for e1, e2 in zip([x.species for x in self.ds.row_iterator(0, 3)], ["virginica", "setosa", "versicolor"]):
            self.assertEqual(e1, e2)

        [x.species for x in self.ds.row_iterator(0, 3)]
        [x.petal_length for x in self.ds.row_iterator(0, 3)]
        with self.assertRaises(AttributeError):
            [x.petal_width for x in self.ds.row_iterator(0, 3)]
        self.ds.use_columns_reset()

    def test_random(self):
        e = self.ds.get_random_sample()
        self.assertEqual(len(e), 1)

        e = self.ds.get_random_sample(10)
        self.assertEqual(len(e), 10)

        e1 = self.ds.get_random_sample(10)
        e2 = self.ds.get_random_sample(10)
        # this could be true if the same sample is picked out, but the probability is low
        self.assertFalse(e1.equals(e2))

        #TODO
        #e = self.ds.filter_columns("species").get_random_sample()

    def test_filter_columns(self):
        # iteration
        for e in self.ds_small.filter_columns(['sepal_length', 'species']):
            e.sepal_length
            e.species
            with self.assertRaises(AttributeError):
                e.petal_width

        # row iteration
        for e in self.ds.filter_columns(['sepal_length', 'species']).row_iterator(2, 10):
            e.sepal_length
            e.species
            with self.assertRaises(AttributeError):
                e.petal_width

        # iloc
        self.ds.filter_columns(['sepal_length', 'species']).iloc[0:10].species
        with self.assertRaises(AttributeError):
            self.ds.filter_columns(['sepal_length', 'species']).iloc[0:10].petal_length
        self.ds.filter_columns(['sepal_length', 'species']).iloc[3].species
        with self.assertRaises(AttributeError):
            self.ds.filter_columns(['sepal_length', 'species']).iloc[3].petal_length
        

class TestDatasetJSON(TestDataset):
    def setUp(self):
        self._client = Client.get_client("127.0.0.1", 5000, method="json")
        self.setup_datasets()


if __name__ == '__main__':
    unittest.main()
