import os
import glob
import itertools

def is_number(string):
    try:
        float(string)
        return True
    except:
        return False

def get_extension(filename):
    filename, extension = os.path.splitext(filename)
    return extension

def first_true_element(l):
    for i, boolean in enumerate(l):
        if boolean == True:
            return i
    return None

def put_in_list_if_not_a_list(possible_list):
    if not isinstance(possible_list, list):
        return [possible_list]
    else:
        return possible_list

def flatten(_list):
    # we do the list to prevent lists of strings of being flattened 
    result = []
    for e in _list:
        if isinstance(e, list):
            result += e
        else:
            result.append(e)
    return result

def expand_glob_if_glob(path):
    if "*" in path:
        # TODO check if really needed
        # sorted needed to get the correct order of the files
        expanded_paths = sorted(glob.glob(path))
        return expanded_paths
    else:
        return path
