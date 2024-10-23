import pandas
import utils

from icecream import ic

def read_one_csv(path, dataset_metadata):
    if dataset_metadata.get('header_in_file') == False:
        dataframe = pandas.read_csv(path, header=None)
    else:
        dataframe = pandas.read_csv(path)
    return dataframe

def read_multiple_csv(paths, dataset_metadata):
    dataframe = None
    for path in paths:
        df1 = dataframe
        df2 = read_one_csv(path, dataset_metadata)
        dataframe = pandas.concat([df1, df2], ignore_index=True) # ignore_index to reindex
    return dataframe

def find_partition_index(index_rows, row):
    return utils.first_true_element([row < i for i in index_rows])

def find_partition_paths(index, first_row, last_row):
    index_rows = list(index.keys())
    index_paths = list(index.values())

    first_row_index = find_partition_index(index_rows, first_row)
    last_row_index = find_partition_index(index_rows, last_row)

    # +1 because we also want the last partition 
    paths = [index_paths[i] for i in range(first_row_index, last_row_index + 1)]
    start = 0 if first_row_index == 0 else index_rows[first_row_index - 1]
    end = index_rows[last_row_index]

    return paths, start, end

def read_dataset_optimized(dataset_metadata, row_input = None, rows_input = None):
    if row_input is not None:
        rows_input = [row_input, row_input]

    first_row, last_row = rows_input

    # determine the partition
    index = dataset_metadata['index']
    paths, partitions_start, partitions_end = find_partition_paths(index, first_row, last_row)
    dataframe = read_multiple_csv(paths, dataset_metadata)

    dataframe.index = range(partitions_start, partitions_end)

    dataframe.columns = dataset_metadata['columns']

    return dataframe

def read_dataset(dataset_metadata):
    paths = dataset_metadata['data_files']
    dataframe = read_multiple_csv(paths, dataset_metadata)
    dataframe.columns = dataset_metadata['columns']
    return dataframe
