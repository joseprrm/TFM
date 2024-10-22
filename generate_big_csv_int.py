from icecream import ic
import string
import random

random.seed(33)

def generate_n_random_ints(n):
    l = []
    for i in range(n):
        l.append(random.randint(0, 100))
    return l

size = 10000000000
#size = 100

number_of_letters = len(string.ascii_letters)
with open('datasets/big_csv_int_1g/csv.csv', 'wt') as f:
    f.write(",".join(string.ascii_letters)+"\n")
    for i in range(size):
        f.write(",".join([str(x) for x in generate_n_random_ints(number_of_letters)])+"\n")

