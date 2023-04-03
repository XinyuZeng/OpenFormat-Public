import numpy as np
import matplotlib.pyplot as plt
import sys
import os
from parse_cpu_profile import parse_cpu_file

cpu_numbers = [1, 2, 4, 8, 16, 24, 32]
# columns = ['%usr', '%sys', '%idle']
# columns = ['%idle']
columns = ['%usr', '%sys', '%iowait', '%idle']
active_columns = ['%usr', '%sys', '%iowait']


def main():
    directory_name = sys.argv[1]
    result = {}
    for column in columns:
        result[column+"_mean"] = []
        result[column+"_max"] = []
        result[column+"_min"] = []
    for cpu_num in cpu_numbers:
        result_cpu = parse_cpu_file(os.path.join(
            directory_name, 'cpu_%d00.log' % cpu_num))
        for column in columns:
            if column in active_columns:
                result[column +
                       "_mean"].append(np.mean(result_cpu[column]) * 32 / cpu_num)
                result[column +
                       "_max"].append(np.max(result_cpu[column]) * 32 / cpu_num)
                result[column +
                       "_min"].append(np.min(result_cpu[column]) * 32 / cpu_num)
            else:
                # result[column].append((np.mean(result_cpu[column]) - (32 - cpu_num) / 32 * 100) * 32 / cpu_num)
                result[column+"_mean"].append(np.mean(result_cpu[column]))
    result['%usr+%sys_mean'] = np.add(result['%usr_mean'], result['%sys_mean'])
    print(result)
    # for column in columns:
    plt.plot(cpu_numbers, result['%usr+%sys_mean'],
             label='(%usr+%sys)_mean_normalized')
    plt.plot(cpu_numbers, result['%iowait_mean'], label='%iowait')
    # plt.plot(cpu_numbers, result['%idle_mean'], label='%idle')
    # plt.plot(cpu_numbers, 100 - np.array(cpu_numbers) * 100 / 32)
    plt.legend()
    plt.savefig('draw_cpu_output.jpg')


if __name__ == '__main__':
    main()
