import numpy as np
import matplotlib.pyplot as plt
import sys
import os
from parse_cpu_profile import parse_cpu_file
from parse_io_profile import parse_io_file
from utils import enumerate_config, parse_output, collect_results

# cpu_numbers = [1, 2, 4, 8, 16, 24, 32]
# columns = ['%usr', '%sys', '%idle']
# columns = ['%idle']
columns = ['%usr', '%sys', '%iowait', '%idle']
active_columns = ['%usr', '%sys', '%iowait']
io_columns = ['tps', 'kB_read/s']


# process all the cpu and io logs into one single csv files, with avg, min, max calculated.
def main():
    os.system('rm outputs/stats.json')
    exp_name = 'pq_encoding_compression'
    directory_name = "./result/profiling/pq_encoding_compression"
    exps = enumerate_config("./experiments/" + exp_name + ".json")
    for exp in exps:
        param_string = "_".join([str(i) for i in list(exp.values())])
        result_cpu = parse_cpu_file(os.path.join(
            directory_name, 'cpu_{}.log'.format(param_string)))
        _, result_device, _ = parse_io_file(os.path.join(
            directory_name, 'io_{}.log'.format(param_string)))
        result = exp.copy()
        for column in columns:
            if column in active_columns:
                result[column +
                       "_mean"] = np.mean(result_cpu[column])
                result[column +
                       "_max"] = np.max(result_cpu[column])
                result[column +
                       "_min"] = np.min(result_cpu[column])
            else:
                # result[column].append((np.mean(result_cpu[column]) - (32 - cpu_num) / 32 * 100) * 32 / cpu_num)
                result[column+"_mean"] = np.mean(result_cpu[column])
        for column in io_columns:
            result[column+'_mean'] = np.mean(result_device[column][1:])
            result[column+'_max'] = np.max(result_device[column][1:])
            result[column+'_min'] = np.min(result_device[column][1:])
        result['%usr+%sys_mean'] = np.add(result['%usr_mean'],
                                          result['%sys_mean'])
        parse_output(result)
    collect_results()
    os.system('mv outputs/stats.csv outputs/{}.csv'.format('cpu_' + exp_name))


if __name__ == '__main__':
    main()
