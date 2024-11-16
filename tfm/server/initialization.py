import os

import yaml

from dataset import Dataset

def initialize_dataset_dict():
    """
    Initializes the Dataset dictionary by reading the configuration files and getting metadata of the datasets
    """
    print("Reading datasets metadata")
    datasets_base_path = './datasets'
    dataset_directory_names = os.listdir(datasets_base_path)
    dataset_directory_paths = [os.path.join(datasets_base_path, directory) for directory in dataset_directory_names]

    datasets = {}
    for directory_name, directory_path in zip(dataset_directory_names, dataset_directory_paths):
        try:
            with open(os.path.join(directory_path,'config.yaml'), 'rt') as file:
                config = file.read()
                config = yaml.safe_load(config)
                if name := config['dataset'].get('name'):
                    pass
                else:
                    name = directory_name
        except FileNotFoundError:
            config = {}
            name = directory_name

        datasets[name] = Dataset(config, directory_path, name)

    return datasets
