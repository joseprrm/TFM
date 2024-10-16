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
