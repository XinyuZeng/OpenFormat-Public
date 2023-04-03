import json
from math import isnan
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os
import sys

CONST_REPEAT_LEVEL = 4
CONST_MOST_REPEAT_NUM = 10


def skewness_anlaysis(single_status):
    skew_pattern = ''
    if single_status['unique_num'] == 0:
        skew_pattern = 'null'
    elif single_status['unique_num'] == 1:
        skew_pattern = 'single'
    elif single_status['unique_num'] == 2:
        skew_pattern = 'binary'
    elif single_status['unique_num'] >= 3 and single_status['zipf'] < 0.01:
        skew_pattern = 'uniform'
    elif single_status['unique_num'] >= 3 and single_status['most_repeated_0'] > 9 * single_status['most_repeated_2']:
        skew_pattern = 'hotspot'
    elif single_status['unique_num'] >= 3 and single_status['zipf'] <= 3 and single_status['zipf'] >= 0.01:
        skew_pattern = 'gentle_zipf'
    else:
        skew_pattern = 'gentle_zipf'
        print('🤔 Other skew pattern but logged as gentle_zipf')

    bi_skew = single_status['most_repeated_0']
    hotspot_first = single_status['most_repeated_0']
    hotspot_second = single_status['most_repeated_1']
    hotspot_third = single_status['most_repeated_2']
    zipf_skew = single_status['zipf']

    res_dict = {}
    res_dict['skew_pattern'] = skew_pattern
    res_dict['bi_skew'] = bi_skew
    res_dict['hotspot_first'] = hotspot_first
    res_dict['hotspot_second'] = hotspot_second
    res_dict['hotspot_third'] = hotspot_third
    res_dict['zipf_skew'] = zipf_skew
    return res_dict


if __name__ == '__main__':
    stats_file_name = '../../../feature/outputs/stats.csv'
    df = pd.read_csv(stats_file_name, sep=',')
    # get the row where "file_name" is file_name
    data_file_name = sys.argv[1]
    data_col_idx = int(sys.argv[2])
    row_num = int(sys.argv[3])
    single_col_status = df[(df['file_name'] == data_file_name) & (df['col_idx'] == data_col_idx)].iloc[0]
    if data_file_name.split('.')[-1] == 'tsv':
        data_base_name = data_file_name.split('.tsv')[0]
    else:
        data_base_name = data_file_name.split('.')[0]

    basic_config, cnt_fea, null_fea, sort_fea, width_fea = {}, {}, {}, {}, {}
    dtype_dict = {'int64': 'int', 'float64': 'float', 'object': 'string', 'bool': 'bool'}
    basic_config['data_type'] = dtype_dict[single_col_status['dtype']]
    basic_config['row_num'] = row_num
    basic_config['target_file_name'] = f"{data_base_name}_{data_col_idx}"
    cnt_fea['cardinality'] = single_col_status['cardinality']
    cnt_fea['unique_num'] = int(single_col_status['unique_num'])
    null_fea['null_ratio'] = single_col_status['null_ratio']

    sort_fea['sort_window'] = 512
    sort_fea['sort_score'] = single_col_status['sortedness_mean']
    # sort_fea['sort_skew'] = single_col_status['sortedness_std']
    # sort_fea['continue_repeat_mean'] = single_col_status['continue_mean']

    repeat_fea = {}
    repeat_num_ratio_list = []
    repeat_num_cut = [0, 1, 2, 3, 5, 10, 20, 50, 100, 200, 300, 400, 512]
    for idx in range(len(repeat_num_cut) - 1):
        tmp_ratio = single_col_status[f'({repeat_num_cut[idx]}, {repeat_num_cut[idx+1]}]_repeated']
        repeat_num_ratio_list.append(tmp_ratio)
    repeat_fea['repeat_num_cut'] = repeat_num_cut
    repeat_fea['repeat_num_ratio_list'] = repeat_num_ratio_list

    width_fea['width_center'] = int(single_col_status['width_mean'])
    if isnan(single_col_status['width_most_repeated_0']):
        width_fea['width_skew'] = 1
    else:
        width_fea['width_skew'] = single_col_status['width_most_repeated_0']

    config_dict = {}
    config_dict['basic_config'] = basic_config
    config_dict['cnt_fea'] = cnt_fea
    config_dict['null_fea'] = null_fea
    config_dict['sort_fea'] = sort_fea
    config_dict['repeat_fea'] = repeat_fea
    config_dict['width_fea'] = width_fea
    config_dict['skew_fea'] = skewness_anlaysis(single_col_status)
    # write to json file
    with open('single_col_config.json', 'w') as f:
        json.dump(config_dict, f, indent=4)