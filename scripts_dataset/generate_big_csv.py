from icecream import ic
import string
import random

random.seed(33)

def generate_n_random_floats(n):
    l = []
    for i in range(n):
        l.append(random.random())
    return l

def generate_n_random_ints(n):
    l = []
    for i in range(n):
        l.append(random.randrange(10000))
    return l

size = 10000000000

columns = ["col1", "col2", "col3", "col4", "col5"]
number_of_letters = len(columns)
with open('datasets/big/big.csv', 'wt') as f:
    f.write(",".join(columns))
    f.write("\n")
    for i in range(size):
        f.write(",".join([str(x) for x in generate_n_random_ints(len(columns))])+"\n")

