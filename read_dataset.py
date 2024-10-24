import pandas

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
    if row_input is not None:
        rows_input = [row_input, row_input]

    first_row, last_row = rows_input

    paths, partitions_start, partitions_end = dataset_metadata['index'].find_partition_paths(first_row, last_row)
    dataframe = read_multiple_csv(paths, dataset_metadata)

    dataframe.index = range(partitions_start, partitions_end)

    dataframe.columns = dataset_metadata['columns']

    return dataframe

def read_dataset(dataset_metadata):
    paths = dataset_metadata['data_files']
    dataframe = read_multiple_csv(paths, dataset_metadata)
    dataframe.columns = dataset_metadata['columns']
    return dataframe
