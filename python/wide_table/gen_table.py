import numpy as np
import sys
import csv
import random

NUM_ROWS = 1000 * 1000
RUN_LEN = 2
BIT_WIDTH_BASE_VAL = 0

random.seed(37)

NUM_INT = 100

int_list = []
for i in range(NUM_INT):
    int_list.append(random.randint(0, 1000 * 1000 * 1000))

def test(NUM_COLS = 20):
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
    with open(f'idx_run{RUN_LEN}_max{BIT_WIDTH_BASE_VAL + 9}_c{NUM_COLS}.csv', "w") as f:
        writer = csv.writer(f)
        for r in result:
            writer.writerow(['a' + str(r)] * NUM_COLS)

if __name__ == '__main__':
    NUM_COLS = int(sys.argv[1])
    test(NUM_COLS)