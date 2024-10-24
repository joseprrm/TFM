import os
import yaml
import pandas
from utils import is_number, get_extension
from icecream import ic 
import glob

def generate_column_names(dataframe):
    columns = []
    for i in range(len(dataframe.columns)):
        columns.append(f"X{i}")
    return columns

def get_columns(dataset_metadata):
    if dataset_metadata.get('header_in_file') == False:
        dataframe = pandas.read_csv(dataset_metadata['data_files'][0], header=None, nrows=0)
        return generate_column_names(dataframe)
    else:
        dataframe = pandas.read_csv(dataset_metadata['data_files'][0], header=0, nrows=0)
        # check if some column is a number  
        if any([is_number(x) for x in dataframe.columns]):
            # header is not in file TODO refactor
            dataset_metadata["header_in_file"] = False
            # some column is a number, so probably the csv is missing the headers, so we setup made up column names
            return generate_column_names(dataframe)
        else:
            # header is in file. TODO refactor
            dataset_metadata["header_in_file"] = True
            # if not, use the headers as read by pandas
            return list(dataframe.columns)

def read_config_file(path):
    with open(path) as f:
        config = f.read()
    config = yaml.safe_load(config)
    return config

def fill_data_files(dataset_metadata):
    files = os.listdir(dataset_metadata['path_to_directory'])
    data_files = [file for file in files if get_extension(file) == ".csv"]
    # sorted needed to get the correct order of the files
    dataset_metadata['data_files'] = sorted([os.path.join(dataset_metadata['path_to_directory'], file) for file in data_files])

def complete_paths_in_index(dataset_metadata):
    for last_row, path in dataset_metadata['index'].items():
        if len(path.split("/")) < 3: 
            path = os.path.join(dataset_metadata['path_to_directory'], path)
            dataset_metadata['index'][last_row] = path

def fill_data_files_short(dataset_metadata):
    # search for data_files that is only a filename, and fix it so it its the whole path
    # when split, it should give at least 3 parts datasets/<dataset>/<data.csv>
#   dataset_metadata['data_files'] = [os.path.join(dataset_metadata['path_to_directory'], file) if (len(file.split("/")) < 3) else file for file in dataset_metadata['data_files']]
    l = []
    for file in dataset_metadata['data_files']:
        if (len(file.split("/")) < 3): 
            l.append(os.path.join(dataset_metadata['path_to_directory'], file))
        else:
            l.append(file)
    dataset_metadata['data_files'] = l

def fill_columns(dataset_metadata):
    if dataset_metadata.get("columns") is None:
        dataset_metadata["columns"] = get_columns(dataset_metadata)

def expand_glob_in_data_files(dataset_metadata):
    full_paths = []
    for path in dataset_metadata['data_files']:
        if "*" in path:
            # if there is a glob, expand and collect the paths
            glob_expression = path
            expanded_paths = sorted(glob.glob(glob_expression, root_dir=dataset_metadata['path_to_directory']))
            expanded_paths_full = [os.path.join(dataset_metadata['path_to_directory'], expanded_path) for expanded_path in expanded_paths]
            full_paths += expanded_paths_full
        else:
            # if there is not glob, collect the path
            full_paths.append(path)

    dataset_metadata['data_files'] = full_paths

def put_string_data_path_in_list(dataset_metadata):
    # if data_files is not a list, make it a list
    if not isinstance(dataset_metadata['data_files'], list):
        dataset_metadata['data_files'] = [dataset_metadata['data_files']]

def init():
    # TODO: put it in a config file for the whole service
    # base path for the datasets driectory
    dataset_directories_path = './datasets'

    dataset_directories = os.listdir(dataset_directories_path)
    data_filesset_directories = [os.path.join(dataset_directories_path, directory) for directory in dataset_directories]

    dataset_metadatas = {}
    for directory_name, directory_path in zip(dataset_directories, data_filesset_directories):
        # save the name of the dataset, and set it up after the if else
        dataset_name = None

        # check if there is a config file
        files = os.listdir(directory_path)
        if "config.yaml" in files:
            path_to_config =  os.path.join(directory_path, "config.yaml")
            config = read_config_file(path_to_config)

            # if the name field exists use it as the key of the dictionary, if not, use the directory name
            if (_tmp := config["dataset"].get("name")) is not None:
                dataset_name = _tmp
            else:
                dataset_name = directory_name

            dataset_metadatas[dataset_name] = config["dataset"]
            dataset_metadatas[dataset_name]["name"] = dataset_name

        else:
            # if there is no config.yaml, use the directory as the name for the dataset
            dataset_name = directory_name
            dataset_metadatas[dataset_name] = {}

        dataset_metadatas[dataset_name]["path_to_directory"] = directory_path
    # At this point, all datasets exist in the dataset_metadatas. The ones with config have the config, the other ones are empty 

    # the datasets with config.yaml should have its fields in their dataset_metadata
    # we need to fill the missing info, expand globs, ...
    for dataset_metadata in dataset_metadatas.values():
        if dataset_metadata.get('data_files') is not None:
            put_string_data_path_in_list(dataset_metadata)
            expand_glob_in_data_files(dataset_metadata)
        if dataset_metadata.get('data_files') is None:
            fill_data_files(dataset_metadata)

        fill_data_files_short(dataset_metadata)
        fill_columns(dataset_metadata)

        if dataset_metadata.get('optimized') == True:
            complete_paths_in_index(dataset_metadata)


    #ic(dataset_metadatas['big_csv_int_1g_split'])
    return dataset_metadatas
