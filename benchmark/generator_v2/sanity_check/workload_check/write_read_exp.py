import json
import pandas as pd
import os
import time
import pyarrow as pa
import pyarrow.orc as po
import pyarrow.parquet as pp


def parse_output(output_stats):
    os.makedirs(os.path.dirname("outputs/stats.json"), exist_ok=True)
    stats = open("outputs/stats.json", 'a+')
    stats.write(json.dumps(output_stats) + "\n")
    stats.close()


def collect_results():
    data = []
    filename = "outputs/stats.json"
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    f = open(filename)
    for line in f:
        data.append(json.loads(line.strip()))
    df = pd.DataFrame(data=data)
    df.to_csv("outputs/stats.csv", index=False)
    os.system("rm outputs/stats.json")


def read_csv(csv_name, single_col=None):
    begin = time.time()
    # 👀, Plz pay attention to the separator
    df = pd.read_csv(csv_name, sep='\t', header=None, skiprows=1, on_bad_lines='warn', low_memory=False)
    # long_str_col = [80, 82]
    # df.drop(df.columns[long_str_col], axis=1, inplace=True)
    pa_tbl = pa.Table.from_pandas(df)
    return pa_tbl, time.time() - begin


def write_orc(pa_tbl, orc_name, exp_stats):
    orc_config = {'dictionary_key_size_threshold': 1, 'compression': 'UNCOMPRESSED', 'compression_strategy': None}
    begin = time.time()
    po.write_table(pa_tbl, orc_name, **orc_config)
    print("write orc time (s):", time.time() - begin)
    exp_stats["write_orc_time (s)"] = time.time() - begin
    # get file size
    # os.path.getsize(orc_name)
    exp_stats["orc_size(MB)"] = os.path.getsize(orc_name) / 1024 / 1024
    return exp_stats


def write_pqt(pa_tbl, pqt_name, exp_stats):
    parquet_config = {'version': '2.6', 'use_dictionary': True, 'compression': 'NONE', 'compression_level': None}
    begin = time.time()
    pp.write_table(pa_tbl, pqt_name, **parquet_config)
    print("write pqt time (s):", time.time() - begin)
    exp_stats["write_pqt_time (s)"] = time.time() - begin
    # get file size
    # os.path.getsize(pqt_name)
    exp_stats["pqt_size(MB)"] = os.path.getsize(pqt_name) / 1024 / 1024
    return exp_stats


def do_all_exp(csv_file_name, target_dir, target_file_name):
    pa_tbl, read_time = read_csv(csv_file_name)
    exp_stats = {"file_name": csv_file_name}
    exp_stats = write_orc(pa_tbl, os.path.join(target_dir, "orc", target_file_name + ".orc"), exp_stats)
    exp_stats = write_pqt(pa_tbl, os.path.join(target_dir, "pqt", target_file_name + ".parquet"), exp_stats)
    # parse_output(exp_stats)


if __name__ == "__main__":
    pass
    # collect_results()