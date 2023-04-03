from threading import local
import numpy as np
import string
import random
import csv
import sys

from gen_data import generate_column

ROW_GROUP_SIZE = 1 * 1024 * 1024

# python3 get_dict_idx.py 20 1000000000 2
if __name__ == '__main__':
    strlen = int(sys.argv[1])
    num_rows = int(sys.argv[2])
    # num_cols = int(sys.argv[3])
    zipf_a = float(sys.argv[3])
    col = generate_column(strlen, num_rows, zipf_a)
    local_dict = {}
    result = []
    for i, data in enumerate(col):
        if i % ROW_GROUP_SIZE == 0:
            local_dict.clear()
        if data not in local_dict:
            local_dict[data] = len(local_dict)
        result.append(local_dict[data])
    # write result to txt file
    with open(f'idx_zipf{zipf_a}.txt', 'w') as f:
        for i in result:
            f.write(str(i) + '\n')