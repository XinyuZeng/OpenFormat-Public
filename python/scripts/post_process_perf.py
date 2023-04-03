import pyarrow.orc as po
import pyarrow as pa
from pyarrow import csv
import pyarrow.dataset as ds
import sys
import time
import os
from utils import parse_config, enumerate_config, parse_output, collect_results
import json
import pandas as pd
import matplotlib.pyplot as plt


def snappy_dict(name, output_dir, file_format):
    df = pd.read_csv(name)
    # print(df)
    if file_format == 'pq':
        df = df[df['data_page_version'] == 2.0]
        df = df[df['version'] == 2.6]
        df_avg = df.groupby(['compression', 'use_dictionary'], as_index=False).mean(
        ).set_index(['compression', 'use_dictionary'])
    else:
        df_avg = df.groupby(['compression', 'dictionary_key_size_threshold'], as_index=False).mean(
        ).set_index(['compression', 'dictionary_key_size_threshold'])

    # print(df_avg)
    fig = plt.figure(figsize=(15, 10))  # Create matplotlib figure
    target_col = 'read_pq_time (s)' if file_format == 'pq' else 'read_orc_time (s)'
    target_col2 = 'file_size (MB)'
    ax = fig.add_subplot(111)  # Create matplotlib axes
    ax2 = ax.twinx()  # Create another axes that shares the same x-axis as ax.
    df_avg[target_col].plot(kind='line', color='grey', legend=True,
                            fontsize=16, ax=ax).legend(loc='upper left')
    df_avg[target_col2].plot(kind='line', legend=False, fontsize=16,
                             ax=ax2, color='brown').legend(loc='upper right')
    ax.set_ylabel("second", fontsize=16)
    ax2.set_ylabel("MB", fontsize=16)
    ax.set_ylim(ymin=0, ymax=1.05*max(df_avg[target_col]))
    ax2.set_ylim(ymin=0, ymax=1.1*max(df_avg[target_col2]))
    ax.xaxis.set_tick_params(rotation=0)
    ax2.xaxis.set_tick_params(rotation=0)
    ax.xaxis.label.set_size(16)
    for i, v in enumerate(df_avg[target_col]):
        ax.text(i-0.2, v + 0.05,  '%.2f' % v,
                color='grey', fontweight='bold', fontsize=16)

    for i, v in enumerate(df_avg[target_col2]):
        ax2.text(i-0.01, v + 6,  '%.2f' % v,
                 color='brown', fontweight='bold', fontsize=16)
    plt.title(name)
    # plt.show()
    plt.savefig(f'{output_dir}/' + name[:-4].split('/')[-1] + '.jpeg')


def breakdown_perf(dict, output_NAME, output_dir):
    num_samples = dict['num_samples']
    labels = []
    sizes = []

    for x, y in dict.items():
        if x == 'num_samples':
            continue
        labels.append(f'{x}_{(num_samples*y/100.0):.1f}')
        sizes.append(y)
    plt.clf()
    # Plot
    fig = plt.figure()
    plt.pie(sizes, labels=labels, textprops={'fontsize': 6})

    plt.axis('equal')
    fig.suptitle(f'{output_NAME}_total_samples_{num_samples}', fontsize=8)
    # plt.show()
    plt.savefig(f'{output_dir}/' + output_NAME + '.jpeg')


def post_process_perf(file_name):
    result = {}
    # read line by line
    with open(file_name) as f:
        lines = f.readlines()
        for line in lines:
            line = line.strip('\n')
            if '# Event count' in line:
                result['num_samples'] = int(line.split(':')[-1].strip())/1000000
                continue
            if '[.]' in line:
                val = float(line.split()[0].strip('%'))
                if val > 3:
                    key = line.split('[.]')[-1].strip()
                    namespaces = key.split('::')
                    if len(namespaces) > 3:
                        key = '::'.join(namespaces[-3:])
                    result[key
                           ] = float(line.split()[0].strip('%'))
                continue
    return result


if __name__ == "__main__":
    name = sys.argv[1]
    exp_name = sys.argv[2]
    file_format = sys.argv[3]
    exps = enumerate_config("./experiments/" + exp_name + ".json")
    for i, exp in enumerate(exps):
        print("----Running experiment:----")
        param_string = "_".join([str(i) for i in list(exp.values())])
        print(param_string)
        result = post_process_perf(
            f'outputs/{file_format}_{name}_{param_string}.perfreport')
        breakdown_perf(
            result, f'{file_format}_breakdown_{name}_{param_string}', 'figures')
    snappy_dict(
        f'./outputs/{file_format}_{name}_{exp_name}.csv', './figures', file_format)
