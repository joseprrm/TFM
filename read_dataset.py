import pandas
import utils

def read_one_csv(path, dataset_metadata):
    if dataset_metadata.get('header_in_file') == False:
        dataframe = pandas.read_csv(path, header=None)
    else:
        dataframe = pandas.read_csv(path)
    return dataframe

def read_dataset_optimized(dataset_metadata, row_input = None, rows_input = None):
    paths = dataset_metadata['data_files']
    rows = dataset_metadata['rows']

    if row_input is not None:
        rows_input = [row_input, row_input]

    first_row, last_row = rows_input

    # determine the partition
    rows = [list(x.keys())[0] for x in rows]

    index_first_row = utils.first_true_element([first_row < i for i in rows])
    index_last_row = utils.first_true_element([last_row < i for i in rows])

    dataframe = None # needs to be defined for the first iteration
    for index in range(index_first_row, index_last_row + 1):
        df1 = dataframe
        df2 = read_one_csv(paths[index], dataset_metadata)
        dataframe = pandas.concat([df1, df2], ignore_index=True) # ignore_index to reindex

    start = 0 if index_first_row == 0 else rows[index_first_row - 1]
    end = rows[index_last_row]
    dataframe.index = range(start, end)

    dataframe.columns = dataset_metadata['columns']

    return dataframe

def read_dataset(dataset_metadata):
    paths = dataset_metadata['data_files']

    dataframe = None
    for path in paths[0:]:
        df1 = dataframe
        df2 = read_one_csv(path, dataset_metadata)
        dataframe = pandas.concat([df1, df2], ignore_index=True) # ignore_index to reindex

    dataframe.columns = dataset_metadata['columns']
    return dataframe
