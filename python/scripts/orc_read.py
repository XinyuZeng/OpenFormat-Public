import pyarrow.orc as po
import pyarrow as pa
from pyarrow import csv
import pyarrow.dataset as ds
import sys
import time
import os
from utils import parse_config, enumerate_config, parse_output, collect_results


def read_orc_raw(file_name, exp_stats):
    begin = time.time()
    os.system(f"perf record -F 997 -g -e cpu-clock /root/orc/build/tools/src/orc-scan {file_name} > tmp.txt")
    tmp_file = open("tmp.txt", "r")
    output = tmp_file.readlines()[0]
    exp_stats["read_orc_time (s)"] = output.split(':')[-1].strip()
    print("read orc time (s):", exp_stats["read_orc_time (s)"])
    print("file size:", os.path.getsize(file_name)/1024/1024, "MB")
    exp_stats["file_size (MB)"] = os.path.getsize(file_name)/1024/1024
    return exp_stats


# FIXME: csv_name needs better parse
# current supported options: single column, write from pq
if __name__ == "__main__":
    os.system('rm outputs/stats.json')
    name = sys.argv[1]
    csv_name = "./data_gen/{}.csv".format(name)
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

    output_stats_list = []
    for i, exp in enumerate(exps):
        print("----Running experiment:----")
        param_string = "_".join([str(i) for i in list(exp.values())])
        print(param_string)
        orc_name = "./{}_{}.orc".format(name, param_string)
        os.system('sync; echo 3 > /proc/sys/vm/drop_caches # drop mem cache')
        time.sleep(3)
        output_stats = exp.copy()
        output_stats = read_orc_raw(orc_name, output_stats.copy())
        os.system(
            f'perf script | stackcollapse-perf.pl | flamegraph.pl > outputs/orc_{name}_{param_string}.svg')
        os.system(
            f'perf report --call-graph --stdio -G > outputs/orc_{name}_{param_string}.perfreport')
        # output_stats["file_size (MB)"] = os.path.getsize(orc_name)/1024/1024
        parse_output(output_stats)
    collect_results()
    os.system(f'mv outputs/stats.csv outputs/orc_{name}_{exp_name}.csv')
