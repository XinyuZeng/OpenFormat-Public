import os
from time import sleep

tbl_name = 'Medicare1_1'
csv_name = f'tables/{tbl_name}.csv'
exp_name = 'pq_encoding_compression'

file = open(csv_name[:-4])
attribute = [line.strip('\n') for line in file.readlines()]
file.close()

for col in attribute:
    os.system(f'python3 scripts/parquet_exp.py {tbl_name} {exp_name} -c={col}')
    sleep(1)
