from io import StringIO
import zlib
import pickle
import json
import base64
import requests
import pandas
from icecream import ic

from utils import ProgrammingError

# for scikit learn
class LazyFloat:
    def __init__(self, func, args, name):
        self._args = args
        self._func = func
        self._value = None
        self._evaluated = False
        self.name = name

    def value(self):
        if not self._evaluated:
            if self._args['column'] == 'species':
                self._value = int(self._func(self.name, **self._args))
            else:
                self._value = float(self._func(self.name, **self._args))
            self._evaluated = True
        return self._value

    def __float__(self):
#        traceback.print_stack()
#        sys.exit()
        return self.value()

    def __int__(self):
        return self.value()

class ILocIndexer():
    def __init__(self, dataset):
        self.dataset = dataset

    def __getitem__(self, key):
        match key:
            case slice():
                return self.dataset.client.read_csv(self.dataset.name,
                                                    rows=(key.start, key.stop), step=key.step,
                                                    columns = self.dataset.columns_filter)
            case int():
                return self.dataset.client.read_csv(self.dataset.name,
                                                    row=key,
                                                    columns = self.dataset.columns_filter)
            case _:
                raise ProgrammingError


class DatasetIterator():
    def __init__(self, dataset):
        self.dataset = dataset
        self.number_of_rows = dataset.number_of_rows()
        self.index = 0

    def __next__(self):
        if self.index < self.number_of_rows:
            tmp = self.dataset.client.read_csv(self.dataset.name,
                                               row=self.index,
                                               columns=self.dataset.columns_filter)
            self.index += 1
            return tmp
        else:
            raise StopIteration

class DatasetRowIterator():
    def __init__(self, dataset, row_first, row_last, columns=None):
        self.dataset = dataset
        self.row_first = row_first
        self.row_last = row_last
        self.columns = columns

        self.index = row_first

    def __iter__(self):
        return self

    def __next__(self):
        if self.index < self.row_last:
            tmp = self.dataset[self.index, self.columns]
            self.index += 1
            return tmp
        else:
            raise StopIteration


class Dataset():
    def __init__(self, name, client, filter_columns = None):
        self.name = name
        self.client = client
        self.columns = client.column_names(name)
        self.columns_filter = filter_columns
        self.iloc = ILocIndexer(self)

    def __iter__(self):
        return DatasetIterator(self)

    def filter_columns(self, columns):
        new_dataset = Dataset(self.name, self.client, filter_columns=columns)
        new_dataset.use_columns(columns)
        return new_dataset

    def row_iterator(self, row_first, row_last, columns=None):
        if self.columns_filter:
            columns = self.columns_filter
        return DatasetRowIterator(self, row_first, row_last, columns)

    def number_of_rows(self):
        n = self.client.number_of_rows(self.name)
        return n

    def use_columns(self, columns):
        self.columns_filter = columns

    def use_columns_reset(self):
        self.columns_filter = None

    def get_random_sample(self, number_of_samples=1):
        return self.client.read_csv(self.name, random=True, number_of_samples=number_of_samples,
                                    columns = self.columns_filter)

    def __getitem__(self, selector):
        """
        selector can be:
            str
            [str, ...]
            range()
            int
            tuple(int, str)
            tuple(range(), str)
            tuple(int, [str, ...])
            tuple(range(), [str, ...])
        """
        arguments = {
            "row":None,
            "column":None,
            "columns":None,
            "rows":None,
            "step":None
        }
        match selector:
            case str():
                arguments["column"] = selector
            case list():
                arguments["columns"] = selector
            case int():
                arguments["row"] = selector
            case range():
                arguments["rows"] = (selector.start, selector.stop)
                arguments["step"] = selector.step
            case (selector_rows, selector_columns):
                match selector_columns:
                    case str():
                        arguments["column"] = selector_columns
                    case list():
                        arguments["columns"] = selector_columns
                match selector_rows:
                    case int():
                        arguments["row"] = selector_rows
                    case range() :
                        arguments["rows"] = (selector_rows.start, selector_rows.stop)
                        arguments["step"] = selector_rows.step

        return self.client.read_csv(self.name, **arguments)

    def select(self, rows=None, columns=None):
        selection = None
        if isinstance(rows, int):
            selection = self.client.read_csv(self.name, row=rows, columns=columns)
        else:
            selection = self.client.read_csv(self.name,
                                             rows=(rows.start, rows.stop, rows.step),
                                             columns=columns)
        return  selection

    def prova_scikit_learn(self, columns, r1, r2):
        """
        only for scikit learn
        """
        outer_list = []
        for i in range(r1, r2):
            inner_list = []
            for column in columns:
                inner_list.append(LazyFloat(self.client.read_csv,
                                            {'row':i, 'column':column},
                                            self.name))

            outer_list.append(inner_list)

        result = []
        if len(outer_list[0]) == 1:
            result = [x[0] for x in outer_list]
        else:
            result = outer_list
        return result

#    def __repr__(self):
#        return str(self.client.read_csv(self.name).head())

class Client():
    @classmethod
    def get_client(cls, *args, method="pickle_base64"):
        """
        Factory method to return different types of client.
        """
        match method:
            case "pickle_base64":
                return Client(*args)
            case "pickle":
                return ClientPickle(*args)
            case "pickle_compressed":
                return ClientPickleCompressed(*args)
            case "json":
                return ClientJSON(*args)
            case _:
                raise ProgrammingError

    def __init__(self, server_address, server_port):
        """
        server_address: a string with the IP or hostname of the server
        server_port: intenger with the port number
        """
        self.server_address = server_address
        self.server_port = str(server_port)
        self.base_url = f"http://{server_address}:{server_port}/"

    def get_dataset(self, dataset_name):
        return Dataset(dataset_name, self)

    def read_csv(self, dataset_name, **kwargs):
        """
        Possible arguments:
        row: int
        column: str
        columns: [str, ...]
        rows: (start, end)
        step(to be used with rows): int
        random: boolean o None
        """
        url = f"{self.base_url}/datasets/{dataset_name}"

        # This deletes the elements that are None,
        # so that we don't send them to the server in the json
        query = {k:v for k,v in kwargs.items() if v is not None}
        query = self.add_method(query)

        if query:
            response = requests.get(url, json=query)
        else:
            response = requests.get(url)

        #ic(f"{len(response.content)} bytes")
        #ic(f"{len(response.content)/1024/1024} megabytes")
        contents = self.deserialize(response)
        return contents

    def add_method(self, query):
        query['method'] = 'pickle_base64'
        return query

    def deserialize(self, response):
        data_pickled = base64.b64decode(response.text)
        data = pickle.loads(data_pickled)
        return data

    def list_datasets(self):
        url = f"{self.base_url}/datasets"
        response = requests.get(url)
        return response.text

    def number_of_rows(self, dataset_name):
        url = f"{self.base_url}/datasets/{dataset_name}/number_of_rows"
        response = requests.get(url)
        number_of_rows = response.json()
        return number_of_rows

    def column_names(self, dataset_name):
        url = f"{self.base_url}/datasets/{dataset_name}/column_names"
        response = requests.get(url)
        column_names = response.json()
        return column_names

class ClientJSON(Client):
    def deserialize(self, response):
        data = None
        match response.text.split(",", 1):
            case ["builtin", _json]:
                data = json.loads(_json)
            case ["dataframe", _json]:
                data = pandas.read_json(StringIO(_json), typ = 'frame')
            case ["series", _json]:
                data = pandas.read_json(StringIO(_json), typ = 'series')
            case _:
                raise Exception("PROGRAMMING ERROR")
        return data

    def add_method(self, query):
        query['method'] = 'json'
        return query

class ClientPickle(Client):
    def deserialize(self, response):
        # we take the raw bytes in the http payload
        data = pickle.loads(response.content)
        return data

    def add_method(self, query):
        query['method'] = 'pickle'
        return query

class ClientPickleCompressed(Client):
    def deserialize(self, response):
        # we take the raw bytes in the http payload
        data = pickle.loads(zlib.decompress(response.content))
        return data

    def add_method(self, query):
        query['method'] = 'pickle_compressed'
        return query
