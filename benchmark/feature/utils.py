import json
import pandas as pd
import os


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
    f = open("outputs/stats.json")
    for line in f:
        data.append(json.loads(line.strip()))
    df = pd.DataFrame(data=data)
    df.to_csv("outputs/stats.csv", index=False)
    os.system('rm ./outputs/stats.json')

if __name__ == "__main__":
    collect_results()