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

def read_dataset_optimized(dataset_metadata, row_input = None, rows_input = None):
    index = dataset_metadata['index']
    paths = [list(e.items())[0][1] for e in index]

    if row_input is not None:
        rows_input = [row_input, row_input]

    first_row, last_row = rows_input

    # determine the partition
    rows = [list(x.keys())[0] for x in index]

    index_first_row = utils.first_true_element([first_row < i for i in rows])
    index_last_row = utils.first_true_element([last_row < i for i in rows])

    # +1 because we also want the last partition 
    paths = [paths[i] for i in range(index_first_row, index_last_row + 1)]
    dataframe = read_multiple_csv(paths, dataset_metadata)

    start = 0 if index_first_row == 0 else rows[index_first_row - 1]
    end = rows[index_last_row]
    dataframe.index = range(start, end)

    dataframe.columns = dataset_metadata['columns']

    return dataframe

def read_dataset(dataset_metadata):
    paths = dataset_metadata['data_files']
    dataframe = read_multiple_csv(paths, dataset_metadata)
    dataframe.columns = dataset_metadata['columns']
    return dataframe
