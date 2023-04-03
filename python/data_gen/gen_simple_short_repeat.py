import numpy as np
import sys
import csv
import random

NUM_ROWS = 1000 * 1000
NUM_COLS = 20
RUN_LEN = 4
BIT_WIDTH_BASE_VAL = 0

random.seed(37)

NUM_INT = 100

int_list = []
for i in range(NUM_INT):
    int_list.append(random.randint(0, 1000 * 1000 * 1000))

def test():
    result = []
    data = []
    for i in int_list:
        # 9 consecutive run makes it both RLE for parquet and orc
        for j in range(RUN_LEN):
            data.append(i + BIT_WIDTH_BASE_VAL)
    # data = [0, 0, 0, 0, 0, 1,1,1,1,1,2,2,2,2,2,3,3,3,3,3,4,4,4,4,4,5,5,5,5,5]
    for i in range(NUM_ROWS):
        idx = i % len(data)
        result.append(data[idx])
    with open(f'idx_run{RUN_LEN}_max{BIT_WIDTH_BASE_VAL + 9}.txt', 'w') as f:
        for i in result:
            f.write('a' + str(i) + '\n')
    with open(f'idx_run{RUN_LEN}_max{BIT_WIDTH_BASE_VAL + 9}.csv', "w") as f:
        writer = csv.writer(f)
        for r in result:
            writer.writerow(['a' + str(r)] * NUM_COLS)

def uniform():
    max_val = 1400
    result = np.random.randint(0, max_val, size=NUM_ROWS).tolist()
    with open(f'idx_uniform_max{max_val}.txt', 'w') as f:
        for i in result:
            f.write(str(i) + '\n')

def uniform_mix_rle():
    rle_data = BIT_WIDTH_BASE_VAL
    max_val = 1400
    result = []
    while len(result) < NUM_ROWS:
        result.extend([rle_data] * RUN_LEN)
        result.extend(np.random.randint(0, max_val, size=RUN_LEN).tolist())
    with open(f'idx_mix_max{max_val}.txt', 'w') as f:
        for i in result:
            f.write(str(i) + '\n')

# main
if __name__ == '__main__':
    RUN_LEN = int(sys.argv[1])
    test()