import os

import pandas
from icecream import ic

import index
import utils

def generate_column_names(number_of_columns):
    columns = []
    for i in range(number_of_columns):
        columns.append(f"X{i}")
    return columns

class Dataset():
    def __init__(self, config, directory_path, name):
        self.name = name
        self.directory_path = directory_path

        if tmp := config.get('dataset'):
            self.config = tmp
        else:
            self.config = {}

        if filenames := self.config.get('data_files'):
            filenames = utils.put_in_list_if_not_a_list(filenames)
            paths = map(self.prepend_directory_path_if_needed, filenames)
            paths = utils.flatten(map(utils.expand_glob_if_glob, paths))
        else:
            paths = self.get_csv_paths_from_filesystem()
        self.paths = list(paths)

        # header_in_file
        if (tmp := self.config.get('header_in_file')) is not None:
            self.header_in_file = tmp 
        else: 
            self.header_in_file = self.figure_out_header_in_file()

        self.columns = tmp if (tmp := self.config.get('columns')) else self.figure_out_columns()

        self.optimized = tmp if (tmp := self.config.get('optimized')) else False

        # if it is calculated at some point, save it here, beacause it shouldn't change
        # it can be None 
        self.number_of_rows = self.config.get('number_of_rows')

        if self.optimized:
            last_rows = self.config['index'].keys()
            filenames = self.config['index'].values()
            paths = map(self.prepend_directory_path_if_needed, filenames)
            index_with_paths = dict(zip(last_rows, paths))
            self.index = index.Index(index_with_paths)

        self.partition_cache = {}

    def figure_out_columns(self):
        if self.header_in_file is True:
            dataframe = pandas.read_csv(self.paths[0], header=0, nrows=0)
            return list(dataframe.columns)
        else:
            dataframe = pandas.read_csv(self.paths[0], header=None, nrows=0)
            return generate_column_names(len(dataframe.columns))

    def figure_out_header_in_file(self):
        # read the first line as the header
        dataframe = pandas.read_csv(self.paths[0], header=0, nrows=0)
        if any((utils.is_number(column) for column in dataframe.columns)):
            return False
        else:
            return True

    def get_csv_paths_from_filesystem(self):
        filenames = os.listdir(self.directory_path)
        filenames = [filename for filename in filenames if utils.get_extension(filename) == ".csv"]
        # sorted needed to get the correct order of the files
        paths = sorted([os.path.join(self.directory_path, file) for file in filenames])
        return paths

    def prepend_directory_path_if_needed(self, file):
        if not self.directory_path in file:
            return os.path.join(self.directory_path, file)
        else:
            return file

    def read_one_csv(self, path):
        if (tmp := self.partition_cache.get(path)) is not None:
            return tmp

        print(path)
        if self.header_in_file is False:
            dataframe = pandas.read_csv(path, header=None)
        else:
            dataframe = pandas.read_csv(path)
        self.partition_cache[path] = dataframe
        return dataframe

    def read_multiple_csv(self, paths):
        dataframe = None
        for path in paths:
            df1 = dataframe
            df2 = self.read_one_csv(path)
            dataframe = pandas.concat([df1, df2], ignore_index=True) # ignore_index to reindex
        return dataframe

    def read_optimized(self, row_input = None, rows_input = None):
        if row_input is not None:
            rows_input = [row_input, row_input]

        first_row, last_row = rows_input

        paths, partitions_start, partitions_end = self.index.find_partition_paths(first_row, last_row)
        dataframe = self.read_multiple_csv(paths)

        dataframe.index = range(partitions_start, partitions_end)

        dataframe.columns = self.columns

        return dataframe

    def read(self):
        dataframe = self.read_multiple_csv(self.paths)
        dataframe.columns = self.columns
        return dataframe
