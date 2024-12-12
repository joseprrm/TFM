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



class DatasetIteratorAsync():
    def __init__(self, dataset):
        self.dataset = dataset
        self.number_of_rows = dataset.number_of_rows()
        self.index = 0

    async def __anext__(self):
        if self.index < self.number_of_rows:
            tmp = await self.dataset.client.read_csv_async(self.dataset.name,
                                               row=self.index,
                                               columns=self.dataset.columns_filter)
            self.index += 1
            return tmp
        else:
            raise StopAsyncIteration

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

class DatasetRowIteratorAsync():
    def __init__(self, dataset, row_first, row_last):
        self.dataset = dataset
        self.row_first = row_first
        self.row_last = row_last
        self.number_of_rows = dataset.number_of_rows()
        self.index = row_first 

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.index < self.row_last:
            tmp = await self.dataset.client.read_csv_async(self.dataset.name,
                                               row=self.index,
                                               columns=self.dataset.columns_filter)
            self.index += 1
            return tmp
        else:
            raise StopAsyncIteration

class Dataset():
    def __init__(self, name, client, filter_columns = None):
        self.name = name
        self.client = client
        self.columns = client.column_names(name)
        self.columns_filter = filter_columns
        self.iloc = ILocIndexer(self)

    def __iter__(self):
        return DatasetIterator(self)

    def __aiter__(self):
        return DatasetIteratorAsync(self)

    def filter_columns(self, columns):
        new_dataset = Dataset(self.name, self.client, filter_columns=columns)
        new_dataset.use_columns(columns)
        return new_dataset

    def row_iterator(self, row_first, row_last, columns=None):
        if self.columns_filter:
            columns = self.columns_filter
        return DatasetRowIterator(self, row_first, row_last, columns)

    def row_iterator_async(self, row_first, row_last, columns=None):
        if self.columns_filter:
            columns = self.columns_filter
        return DatasetRowIteratorAsync(self, row_first, row_last)

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

        # if not filtering by columns in getitem, use the columns_filter
        if not arguments["column"] or not arguments["columns"]:
            if self.columns_filter is not None:
                arguments["columns"] = self.columns_filter

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
