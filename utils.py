import os

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
