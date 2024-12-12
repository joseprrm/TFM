from icecream import ic
import yaml
import os
import pandas

header_in_file = True

dataset_path = "datasets/big/big.csv"

output_dir = "datasets/big_partitions/"

template = "{number}.csv"

ROWS_IN_PARTITION = 100000
FILENAME_ZEROS_PADDING = 7

def write_partition(partition, partition_number):
        output_filename = template.format(number=str(partition_number).zfill(FILENAME_ZEROS_PADDING))
        output_path = os.path.join(output_dir, output_filename)
        with open(output_path, "wt") as output_file:
            conents = None
            if header_in_file:
                contents = header + "".join(partition)
            else:
                contents = "".join(partition)
            output_file.write(contents)
        return output_filename

def read_partition(file):
    """
    returns:
        partition: list of lines
        end_of_file: flag to signal the end of the file
    """
    end_of_file = False
    partition = []
    for i in range(ROWS_IN_PARTITION):
        line = file.readline()
        # if line is an empty string, is EOF
        if(line == ""):
            # if end of file, setup the flag and break in order to not put the empty string in the partition
            end_of_file = True
            break
        partition.append(line)
    return partition, end_of_file


config = {'dataset': {'name': 'big_csv_int_1g_split', 'header_in_file': header_in_file, 'index':{}, 'optimized': True}}
total_lines_read = 0

partition_number = 0
_continue = True
with open(dataset_path, 'rt') as file:
    header = None
    if header_in_file:
        header = file.readline()

    end_of_file = False
    counter = 0
    while not end_of_file:
        counter += 1
        print(counter)
        partition, end_of_file = read_partition(file)
        filename = write_partition(partition, partition_number)

        lines_read = len(partition)
        total_lines_read += lines_read

        config['dataset']['index'][total_lines_read] = filename

        partition_number += 1

config['dataset']['number_of_rows'] = total_lines_read

ic(config)
ic(yaml.dump(config))
with open(os.path.join(output_dir, 'config.yaml'), 'wt') as file:
    file.write(yaml.dump(config) + "\n")
