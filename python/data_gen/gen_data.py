from scipy.special import zeta
import matplotlib.pyplot as plt
import scipy
from scipy.stats import skewnorm
import numpy as np
import string
import random
import csv
import sys


def generate_string(length):
    return ''.join(random.choice(
        string.ascii_letters + string.digits) for i in range(length))


def generate_column(str_len, num_rows, zipf_a):
    string_cache = {}
    samples = np.random.zipf(zipf_a, num_rows)
    result = []
    for i in samples:
        if i not in string_cache:
            string_cache[i] = generate_string(str_len)
        result.append(string_cache[i])
    return result


# main function
# example: python3 gen_data.py 20 20000000 10 2
if __name__ == '__main__':
    strlen = int(sys.argv[1])
    num_rows = int(sys.argv[2])
    num_cols = int(sys.argv[3])
    zipf_a = float(sys.argv[4])
    col = generate_column(strlen, num_rows, zipf_a)

    cols = []
    cols += [col]*num_cols
    rows = zip(*cols)
    with open(f'strlen{strlen}_rows{num_rows}_cols{num_cols}_zipf{zipf_a}.csv', 'w') as f:
        writer = csv.writer(f, delimiter=',')
        for row in rows:
            writer.writerow(row)
