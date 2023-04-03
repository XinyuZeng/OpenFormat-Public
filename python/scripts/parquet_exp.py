import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
from pyarrow import csv
import sys
import time
import os
import glob
from utils import *


def write_parquet_from_many_csv(name, parquet_name, config, exp_stats, single_col=None):
    file = open(f'tables/{name}_1')
    attribute = [line.strip('\n') for line in file.readlines()]
    file.close()

    begin = time.time()
    df_list = []
    namesList = glob.glob(f'tables/{name}*.csv')
    print(namesList)
    for f in namesList:
        if single_col:
            df = pd.read_csv(f, names=attribute,
                             sep='|', usecols=[single_col])
        else:
            df = pd.read_csv(f, names=attribute,
                             sep='|')
        df_list.append(df)
    pa_tbl = pa.Table.from_pandas(pd.concat(df_list, ignore_index=True))
    print("read csv time (s):", time.time() - begin)
    exp_stats["read_csv_time (s)"] = time.time() - begin

    begin = time.time()
    pq.write_table(
        pa_tbl, parquet_name, **parse_config(config))
    print("write parquet time (s):", time.time() - begin)
    exp_stats["write_parquet_time (s)"] = time.time() - begin
    return exp_stats


def write_parquet(pa_tbl, parquet_name, config, exp_stats):
    begin = time.time()
    pq.write_table(
        pa_tbl, parquet_name, **parse_config(config))
    print("write parquet time (s):", time.time() - begin)
    exp_stats["write_parquet_time (s)"] = time.time() - begin
    return exp_stats


def parquet_info(file_name):
    parquet_file = pq.ParquetFile(file_name)
    for attr, value in parquet_file.__dict__.items():
        print(attr, value)
    print(dir(parquet_file))

    for property in dir(parquet_file):
        if property.startswith('_'):
            continue
        print('--INFO--' + property + ':', getattr(parquet_file, property))


def read_parquet(file_name, exp_stats):
    parquet_file = pq.ParquetFile(file_name)
    begin = time.time()
    parquet_file.read()
    print("read parquet time (s):", time.time() - begin)
    exp_stats["read_parquet_time (s)"] = time.time() - begin
    print("file size:", os.path.getsize(file_name)/1024/1024, "MB")
    exp_stats["file_size (MB)"] = os.path.getsize(file_name)/1024/1024
    return exp_stats


# Usage: python3 scripts/parquet_exp.py table_name(e.g Generico_1)
# experiment_name(e.g pq_test) [-w=true|false] [-r=true|false]
# [-o=true|false] [-c=column_name] [-manycsv=true|false]
if __name__ == "__main__":
    os.system('rm outputs/stats.json')
    name = sys.argv[1]
    csv_name = "data_gen/{}.csv".format(name)
    exps = enumerate_config("./experiments/" + sys.argv[2] + ".json")
    exp_name = f"{name}_{sys.argv[2]}"
    single_col = None
    for i in range(3, len(sys.argv)):
        if sys.argv[i].startswith('-w'):
            write = True if sys.argv[i].split('=')[1] == 'true' else False
        if sys.argv[i].startswith('-r'):
            read = True if sys.argv[i].split('=')[1] == 'true' else False
        if sys.argv[i].startswith('-o'):
            collect = True if sys.argv[i].split('=')[1] == 'true' else False
        if sys.argv[i].startswith('-c'):
            single_col = sys.argv[i].split('=')[1]
            exp_name += f'_s_{single_col}'
        if sys.argv[i].startswith('-manycsv='):
            manycsv = True if sys.argv[i].split('=')[1] == 'true' else False

    output_stats_list = []
    pa_tbl, read_time = read_csv(csv_name, single_col)
    for i, exp in enumerate(exps):
        print("----Running experiment: {} out of {}----".format(i+1, len(exps)))
        param_string = "_".join([str(i) for i in list(exp.values())])
        print(param_string)
        parquet_name = "./{}_{}.parquet".format(name, param_string)
        output_stats = {}
        if 'manycsv' in locals() and manycsv:
            output_stats = write_parquet_from_many_csv(
                name, parquet_name, exp, exp.copy(), single_col=single_col)
        elif 'write' not in locals() or write:
            output_stats = write_parquet(
                pa_tbl, parquet_name, exp, exp.copy())
        os.system('sync; echo 3 > /proc/sys/vm/drop_caches # drop mem cache')
        time.sleep(3)
        os.system('iostat 2 > io_profile.log&')
        os.system('mpstat 2 > cpu_profile.log&')
        if 'read' not in locals() or read:
            output_stats = read_parquet(parquet_name, output_stats.copy())
        os.system('pkill -2 mpstat; pkill -2 iostat')
        os.makedirs(os.path.dirname(
            "result/profiling/{}/".format(exp_name)), exist_ok=True)
        os.system('mv io_profile.log %s' % os.path.join(
            'result', 'profiling', exp_name, 'io_{}.log'.format(param_string)))
        os.system('mv cpu_profile.log %s' % os.path.join(
            'result', 'profiling', exp_name, 'cpu_{}.log'.format(param_string)))
        parse_output(output_stats)
    collect_results()
    if 'collect' not in locals() or collect:
        os.system('mv outputs/stats.csv outputs/{}.csv'.format(exp_name))
