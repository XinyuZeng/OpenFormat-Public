import json
import pandas as pd
import os
import time
import pyarrow.csv as csv

def read_csv(csv_name, single_col=None):
    try:
        file = open(csv_name[:-4])
        attribute = [line.strip('\n') for line in file.readlines()]
        file.close()
    except OSError:
        attribute = None
    begin = time.time()
    pa_tbl = csv.read_csv(csv_name, read_options=csv.ReadOptions(
        column_names=attribute, autogenerate_column_names=True), parse_options=csv.ParseOptions(delimiter=','),
        convert_options=csv.ConvertOptions(strings_can_be_null=True, 
        include_columns=attribute if single_col is None else [single_col]))
    return pa_tbl, time.time() - begin

def enumerate_config(json_name):
    '''list type for each config in .json, and it will enumerate all 
    the possible combinations of the config. i.e. the number of tests 
    in total will be the product of the length of every list.'''
    print("loading config from {} ...".format(json_name))
    job = json.load(open(json_name))
    # args = [""]
    args = [{}]
    for key in job:
        new_args = []
        if isinstance(job[key], list):
            for i, x in enumerate(job[key]):
                for arg in args:
                    # arg = arg + "{}={} ".format(key, x)
                    new_arg = arg.copy()
                    new_arg[key] = x
                    new_args.append(new_arg)
        else:
            for arg in args:
                # arg = arg + "{}={} ".format(key, job[key])
                new_arg = arg.copy()
                new_arg[key] = job[key]
                new_args.append(new_arg)
        args = new_args
    return args


def parse_config(config):
    result = {}
    for key, value in config.items():
        if key != "i":
            result[key] = value
    return result


def parse_output(output_stats):
    os.makedirs(os.path.dirname("outputs/stats.json"), exist_ok=True)
    stats = open("outputs/stats.json", 'a+')
    stats.write(json.dumps(output_stats)+"\n")
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
