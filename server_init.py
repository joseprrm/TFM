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

def get_columns(dataset_info):
    dataframe = None
    if dataset_info.get('header_in_file') == False:
        dataframe = pandas.read_csv(dataset_info['data_files'][0], header=None, nrows=0)
        return generate_column_names(dataframe)
    else:
        dataframe = pandas.read_csv(dataset_info['data_files'][0], header=0, nrows=0)
        columns = []
        # check if some column is a number  
        if any([is_number(x) for x in dataframe.columns]):
            # header is not in file TODO refactor
            dataset_info["header_in_file"] = False
            # some column is a number, so probably the csv is missing the headers, so we setup made up column names
            return generate_column_names(dataframe)
        else:
            # header is in file. TODO refactor
            dataset_info["header_in_file"] = True
            # if not, use the headers as read by pandas
            return list(dataframe.columns)

def read_config_file(path):
    config = None
    with open(path) as f:
        config = f.read()
    config = yaml.safe_load(config)
    return config

def fill_data_files(dataset_info):
    files = os.listdir(dataset_info['path_to_directory'])
    data_files = [file for file in files if get_extension(file) == ".csv"]
    # sorted needed to get the correct order of the files
    dataset_info['data_files'] = sorted([os.path.join(dataset_info['path_to_directory'], file) for file in data_files])

def fill_data_files_short(dataset_info):
    # search for data_files that is only a filename, and fix it so it its the whole path
    # when split, it should give at least 3 parts datasets/<dataset>/<data.csv>
#   dataset_info['data_files'] = [os.path.join(dataset_info['path_to_directory'], file) if (len(file.split("/")) < 3) else file for file in dataset_info['data_files']]
    l = []
    for file in dataset_info['data_files']:
        if (len(file.split("/")) < 3): 
            l.append(os.path.join(dataset_info['path_to_directory'], file))
        else:
            l.append(file)
    dataset_info['data_files'] = l

def fill_columns(dataset_info):
    if dataset_info.get("columns") is None:
        dataset_info["columns"] = get_columns(dataset_info)

def expand_glob_in_data_files(dataset_info):
    full_paths = []
    for path in dataset_info['data_files']:
        if "*" in path:
            # if there is a glob, expand and collect the paths
            glob_expression = path
            expanded_paths = sorted(glob.glob(glob_expression, root_dir=dataset_info['path_to_directory']))
            expanded_paths_full = [os.path.join(dataset_info['path_to_directory'], expanded_path) for expanded_path in expanded_paths]
            full_paths += expanded_paths_full
        else:
            # if there is not glob, collect the path
            full_paths.append(path)

    dataset_info['data_files'] = full_paths

def put_string_data_path_in_list(dataset_info):
    # if data_files exists and it is not a list, make it a list
    if isinstance(dataset_info['data_files'], list) == False:
        dataset_info['data_files'] = [dataset_info['data_files']]

def init():
    # TODO: put it in a config file for the whole service
    # base path for the datasets driectory
    dataset_directories_path = './datasets'

    dataset_directories = os.listdir(dataset_directories_path)
    data_filesset_directories = [os.path.join(dataset_directories_path, directory) for directory in dataset_directories]

    dataset_infos = {}
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

            dataset_infos[dataset_name] = config["dataset"]
            dataset_infos[dataset_name]["name"] = dataset_name

        else:
            # if there is no config.yaml, use the directory as the name for the dataset
            dataset_name = directory_name
            dataset_infos[dataset_name] = {}

#        dataset_infos[dataset_name]["directory"] = directory_name
        dataset_infos[dataset_name]["path_to_directory"] = directory_path

    # at this point we should have all the datasets with the correct key in datasets_infos. 
    # the datasets with config.yaml should have its fields in their dataset_info
    # we need to fill the missing info, expand globs, ...
    for dataset_info in dataset_infos.values():
        if dataset_info.get('data_files') is not None:
            put_string_data_path_in_list(dataset_info)
            expand_glob_in_data_files(dataset_info)
        if dataset_info.get('data_files') is None:
            fill_data_files(dataset_info)

        fill_data_files_short(dataset_info)
        fill_columns(dataset_info)

    #ic(dataset_infos)
    return dataset_infos
