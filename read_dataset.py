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
    """
    Find the partition index in the index data structure

    index_rows: list of last row in each partition
    row: row that the caller wants the partition index

    returns: the number of the partition
    """
    return utils.first_true_element([row < i for i in index_rows])

def find_partition_paths(index, first_row, last_row):
    """
    index: index data structure
    first_row: first row in the range
    last_row: last row in the range (exclusive range)

    returns:
        paths: list of the paths of the partitions
        start: first row in the partition group
        end: last row in the partition group (exclusive range)

    Index data structure:
        The index data structure is a dict, but is not used as such.
        keys() hold the "last row + 1" of each partition.
        values() hold the path of each partition file.
    """
    # variables with index_<> refer to index as a the whole "indexing data structure" (index)
    # variables with <>_index refer to the actual index number (an integer) in the "indexing data structure" (index)

    index_rows = list(index.keys())
    index_paths = list(index.values())

    first_row_index = find_partition_index(index_rows, first_row)
    last_row_index = find_partition_index(index_rows, last_row)

    # +1 because we also want the last partition 
    paths = [index_paths[i] for i in range(first_row_index, last_row_index + 1)]

    # start is 0 if first partition is the first of the dataset
    # otherwise, the start is the previous partition "row" (the one that determines the partition in the index)
    start = 0 if first_row_index == 0 else index_rows[first_row_index - 1]
    # end is the partition "row"
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
