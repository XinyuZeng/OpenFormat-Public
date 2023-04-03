import pyarrow.orc as po
import pyarrow.parquet as pq
import pyarrow as pa
from pyarrow import csv
import pyarrow.dataset as ds
import sys
import time
import os
import duckdb
from utils import *

def write_orc(pa_tbl, orc_name, config, exp_stats):
    begin = time.time()
    po.write_table(
        pa_tbl, orc_name, **parse_config(config))
    print("write orc time (s):", time.time() - begin)
    exp_stats["write_orc_time (s)"] = time.time() - begin
    return exp_stats


def write_orc_from_pq(file_name, orc_name, config, exp_stats):
    begin = time.time()
    table = ds.dataset(file_name,
                       format='parquet').to_table()
    po.write_table(
        table, orc_name, **parse_config(config))
    print("write orc time (s):", time.time() - begin)
    exp_stats["write_orc_time (s)"] = time.time() - begin
    return exp_stats


def orc_info(file_name):
    orc_file = po.ORCFile(file_name)
    for attr, value in orc_file.__dict__.items():
        print(attr, value)
    print(dir(orc_file))

    for property in dir(orc_file):
        if property.startswith('_'):
            continue
        print('--INFO--' + property + ':', getattr(orc_file, property))


def read_orc(file_name, exp_stats):
    orc_file = po.ORCFile(file_name)
    begin = time.time()
    orc_file.read()
    print("read orc time (s):", time.time() - begin)
    exp_stats["read_orc_time (s)"] = time.time() - begin
    print("file size (MB):", orc_file.file_length/1024/1024, "MB")
    exp_stats["file_size (MB)"] = orc_file.file_length/1024/1024
    return exp_stats


def read_orc_raw(file_name, exp_stats):
    begin = time.time()
    os.system(f"perf record -F 99 -g ./orc-scan {file_name}")
    print("read orc time (s):", time.time() - begin)
    exp_stats["read_orc_time (s)"] = time.time() - begin
    print("file size:", os.path.getsize(file_name)/1024/1024, "MB")
    exp_stats["file_size (MB)"] = os.path.getsize(file_name)/1024/1024
    return exp_stats


# FIXME: csv_name needs better parse
# current supported options: single column, write from pq
if __name__ == "__main__":
    os.system('rm outputs/stats.json')
    name = sys.argv[1]
    exp_name = sys.argv[2]
    exps = enumerate_config("./experiments/" + exp_name + ".json")
    single_col = None
    from_pq = False
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
        if sys.argv[i].startswith('-p='):
            # write orc from parquet
            from_pq = True if sys.argv[i].split('=')[1] == 'true' else False

    if not from_pq:
        csv_name = "./data_gen/{}.csv".format(name)
    output_stats_list = []
    if not from_pq:
        pa_tbl, read_time = read_csv(csv_name, single_col)
    for i, exp in enumerate(exps):
        print("----Running experiment:----")
        param_string = "_".join([str(i) for i in list(exp.values())])
        print(param_string)
        orc_name = "./{}_{}.orc".format(name, param_string)
        if not from_pq:
            output_stats = write_orc(
                pa_tbl, orc_name, exp, exp.copy())
        else:
            output_stats = write_orc_from_pq(
                name, orc_name, exp, exp.copy())
        os.system('sync; echo 3 > /proc/sys/vm/drop_caches # drop mem cache')
        time.sleep(3)
        # output_stats = read_orc_raw(orc_name, output_stats.copy())
        # os.system(
        #     f'perf script | stackcollapse-perf.pl | flamegraph.pl > outputs/{name}_{param_string}.svg')
        # os.system(
        #     f'perf report --call-graph --stdio -G > outputs/{name}_{param_string}.perfreport')
        # output_stats["file_size (MB)"] = os.path.getsize(orc_name)/1024/1024
        parse_output(output_stats)
    collect_results()
    # os.system(f'mv outputs/stats.csv outputs/{name}_{exp_name}.csv')
