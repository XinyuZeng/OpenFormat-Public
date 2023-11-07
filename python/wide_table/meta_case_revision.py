import os
import sys
import datetime
import pathlib
import pandas as pd
dir_path = pathlib.Path(os.path.abspath('')).resolve()
HOME_DIR = str(dir_path).split('/OpenFormat')[0]

timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

PROJ_SRC_DIR = f'{HOME_DIR}/OpenFormat'
sys.path.insert(1, f'{PROJ_SRC_DIR}')
from python.scripts.utils import *
print(dir_path)

import pyarrow as pa
import random
import pyarrow.parquet as pq
import pyarrow.orc as po
random.seed(1737)

num_rows = 64 * 1000
# proj_rows_list = list(range(1, 101))
proj_rows_list = [10]
# [20, 40, 60, 80, 100]
num_cols_list = list(range(100, 10000+100, 100))
pq_config = enumerate_config(f'{PROJ_SRC_DIR}/python/experiments/pq_default.json')
orc_config = enumerate_config(f'{PROJ_SRC_DIR}/python/experiments/orc_default.json')
id_array = pa.array(list(range(num_rows)), type=pa.int64())

# arrays_100cols = []
# names_100cols = []
# for i in range(100):
    # arrays_100cols.append(pa.array([i for _ in range(num_rows)], type=pa.int64()))
    # names_100cols.append(f'key{i}')
    # arrays_100cols.append(pa.array([random.uniform(0, 1000) for _ in range(num_rows)], type=pa.float64()))
    # names_100cols.append(f'val{i}')

arrays_cur = [id_array]
names_cur = ['user_id']

# 8400 logic
# for i in range(10100):
#     arrays_cur.append(pa.array([i for _ in range(num_rows)], type=pa.int64()))
#     names_cur.append(f'key{i}')
#     arrays_cur.append(pa.array([random.uniform(0, 1000) for _ in range(num_rows)], type=pa.float64()))
#     names_cur.append(f'val{i}')

table = pa.Table.from_arrays(arrays_cur, names=names_cur)
del arrays_cur
del names_cur
# need to restart kernel if mem usage is high
scan_exec_pq = f'{HOME_DIR}/arrow-private/cpp/out/build/openformat-release/release/parquet-scan-columnbatch'
os.system('rm -f outputs/stats.json')
output_stats = {}
# for num_cols in list(range(100, 10100, 100)):
for num_cols in [10000]:
    print(f'begin {num_cols}')
    base_name = f'meta_r{num_rows}_c{num_cols}'
    # for i in range(num_cols-100, num_cols):
    #     table = table.append_column(f'key{i}', pa.array([i for _ in range(num_rows)], type=pa.int64()))
    #     table = table.append_column(f'val{i}', pa.array([random.uniform(0, 1000) for _ in range(num_rows)], type=pa.float64()))
    # remove 100 columns from the table
    # for i in range(num_cols, num_cols+100):
    #     table = table.drop([f'key{i}', f'val{i}'])
    # po.write_table(table, f'{dir_path}/meta_case/{base_name}.orc', **parse_config(orc_config[0]))
    # pq.write_table(table, f'{dir_path}/meta_case/{base_name}.parquet', **parse_config(pq_config[0]))
    os.system(f'scp xinyu@Charlie:/public/xinyu/meta_case_no_key/{base_name}.orc {dir_path}/meta_case/')
    os.system(f'scp xinyu@Charlie:/public/xinyu/meta_case_no_key/{base_name}.parquet {dir_path}/meta_case/')
    for proj_rows in proj_rows_list:
        print(f'begin {proj_rows}')
        sampled_cols = random.sample(range(num_cols), proj_rows)
        idx_list = []
        names_list = []
        for c in sampled_cols:
            idx_list.append(str(c + 1))
            # idx_list.append(str(2*c + 1))
            # idx_list.append(str(2*c + 2))
            # names_list.append(f'key{c}')
            names_list.append(f'val{c}')
        col_idxs = ",".join(idx_list)
        col_names = ",".join(names_list)
        for i in range(5):
            orc_out_lines = os.popen(f"{HOME_DIR}/orc/build/c++/test/FilterExpRevision \
            {dir_path}/meta_case/{base_name}.orc {col_names} float none \
            0 0 one").read().split('\n')
            orc_time = float(orc_out_lines[0].split(' ')[-1])*1000 # ms
            orc_meta_time = float(orc_out_lines[1].split(' ')[-1]) # us
            orc_startNextStripe_time = float(orc_out_lines[2].split(' ')[-1]) # us
            orc_buildReader_time = float(orc_out_lines[3].split(' ')[-1]) # us
            os.system('sync; echo 3 > /proc/sys/vm/drop_caches')
            pq_out_lines = os.popen(f'''{scan_exec_pq} \
                            --batch_size=1024 --columns={col_idxs} {dir_path}/meta_case/{base_name}.parquet''').read().split('\n')
            pq_time = float(pq_out_lines[0].split(' ')[-2]) * 1000
            pq_meta_time = float(pq_out_lines[1].split(' ')[-2]) / 1000 # us
            os.system('sync; echo 3 > /proc/sys/vm/drop_caches')
            output_stats['pq_time'] = pq_time
            output_stats['pq_meta_time'] = pq_meta_time
            output_stats['orc_time'] = orc_time
            output_stats['orc_meta_time'] = orc_meta_time
            output_stats['orc_startNextStripe_time'] = orc_startNextStripe_time
            output_stats['orc_buildReader_time'] = orc_buildReader_time
            output_stats['i'] = i
            output_stats['num_proj_rows'] = proj_rows
            output_stats['num_cols'] = num_cols
            output_stats['num_rows'] = num_rows
            parse_output(output_stats)
    os.remove(f'{dir_path}/meta_case/{base_name}.orc')
    os.remove(f'{dir_path}/meta_case/{base_name}.parquet')
collect_results()
os.system('mv outputs/stats.csv ../outputs/{}_{}.csv'.format('projection_exp_revision', timestamp))