# unique num and cardinality analysis
from enum import unique
import json
from math import isnan
from unicodedata import category
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os
import multi_workloads_config_gen as multi_config

cwi_ratio = 36 / (32 + 36)
noc_ratio = 1 - cwi_ratio


def add_prob_lists(cwi_prob_list, noc_prob_list):
    # each list multiply its ratio and add up
    if isnan(cwi_prob_list[0]):
        return noc_prob_list
    if isnan(noc_prob_list[0]):
        return cwi_prob_list
    cwi_array = np.array(cwi_prob_list) * cwi_ratio
    noc_array = np.array(noc_prob_list) * noc_ratio
    return (cwi_array + noc_array).round(4).tolist()


if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    df = pd.read_csv(multi_config.stats_file_name, sep=',')
    df_cwi = df[df['dataset'] == 'cwi']
    df_noc = df[df['dataset'] != 'cwi']
    cwi_map = multi_config.feature_analysis(df_cwi)
    noc_map = multi_config.feature_analysis(df_noc)
    for dtype in cwi_map.keys():
        cwi_config = cwi_map[dtype]
        noc_config = noc_map[dtype]

        cnt_fea, null_fea, sort_fea, width_fea = {}, {}, {}, {}
        # cnt_fea
        cwi_cnt = cwi_config['cnt_fea']
        noc_cnt = noc_config['cnt_fea']
        SORT_LEVEL = 10
        CAR_LEVEL = 9
        cnt_fea['unique_num'] = cwi_cnt['unique_num']  # it's an interval cut
        cnt_fea['cardinality'] = cwi_cnt['cardinality']  # it's an interval cut
        for i in range(SORT_LEVEL):
            cnt_fea[
                f'is_categorical_sort{i}'] = cwi_cnt[f'is_categorical_sort{i}'] * cwi_ratio + noc_cnt[f'is_categorical_sort{i}'] * noc_ratio
            cnt_fea[f'unique_num_sort{i}_prob'] = add_prob_lists(cwi_cnt[f'unique_num_sort{i}_prob'], noc_cnt[f'unique_num_sort{i}_prob'])
            cnt_fea[f'cardinality_sort{i}_prob'] = add_prob_lists(cwi_cnt[f'cardinality_sort{i}_prob'],
                                                                  noc_cnt[f'cardinality_sort{i}_prob'])
        # null_fea
        cwi_null = cwi_config['null_fea']
        noc_null = noc_config['null_fea']
        null_fea['null_ratio'] = cwi_null['null_ratio']
        null_fea['null_ratio_prob'] = add_prob_lists(cwi_null['null_ratio_prob'], noc_null['null_ratio_prob'])
        # sort_fea
        cwi_sort = cwi_config['sort_fea']
        noc_sort = noc_config['sort_fea']
        sort_fea['sort_window'] = cwi_sort['sort_window']
        sort_fea['sort_score'] = cwi_sort['sort_score']
        sort_fea['sort_score_prob'] = add_prob_lists(cwi_sort['sort_score_prob'], noc_sort['sort_score_prob'])
        # width_fea
        cwi_width = cwi_config['width_fea']
        noc_width = noc_config['width_fea']
        width_fea['width_center'] = cwi_width['width_center']
        for i in range(CAR_LEVEL):
            width_fea[f'width_center_car{i}_prob'] = add_prob_lists(cwi_width[f'width_center_car{i}_prob'],
                                                                    noc_width[f'width_center_car{i}_prob'])
        width_fea['width_skew'] = cwi_width['width_skew']
        width_fea['width_skew_prob'] = add_prob_lists(cwi_width['width_skew_prob'], noc_width['width_skew_prob'])

        # continuous_repeat_fea
        continuous_repeat_fea = {}
        cwi_continuous_repeat = cwi_config['continuous_repeat_fea']
        noc_continuous_repeat = noc_config['continuous_repeat_fea']
        continuous_repeat_fea['continuous_mean_cut'] = cwi_continuous_repeat['continuous_mean_cut']
        continuous_repeat_fea['repeat_num_cut'] = cwi_continuous_repeat['repeat_num_cut']
        continuous_repeat_fea['continue_mean_map_repeat_block_ratio'] = cwi_continuous_repeat['continue_mean_map_repeat_block_ratio']
        for i in range(SORT_LEVEL):
            continuous_repeat_fea[f'continuous_mean_sort{i}_prob'] = add_prob_lists(cwi_continuous_repeat[f'continuous_mean_sort{i}_prob'],
                                                                                    noc_continuous_repeat[f'continuous_mean_sort{i}_prob'])
        # skew_fea
        skew_fea = {}
        cwi_skew_fea = cwi_config['skew_fea']
        noc_skew_fea = noc_config['skew_fea']
        skew_fea['skew_pattern'] = cwi_skew_fea['skew_pattern']
        skew_fea['bi_skew'] = cwi_skew_fea['bi_skew']
        skew_fea['hotspot_first'] = cwi_skew_fea['hotspot_first']
        skew_fea['hotspot_skew'] = cwi_skew_fea['hotspot_skew']
        skew_fea['gentle_zipf_skew'] = cwi_skew_fea['gentle_zipf_skew']
        for i in range(SORT_LEVEL):
            skew_fea[f'skew_pattern_sort{i}_prob'] = add_prob_lists(cwi_skew_fea[f'skew_pattern_sort{i}_prob'],
                                                                    noc_skew_fea[f'skew_pattern_sort{i}_prob'])
            skew_fea[f'bi_skew_sort{i}_prob'] = add_prob_lists(cwi_skew_fea[f'bi_skew_sort{i}_prob'], noc_skew_fea[f'bi_skew_sort{i}_prob'])
            skew_fea[f'hotspot_first_sort{i}_prob'] = add_prob_lists(cwi_skew_fea[f'hotspot_first_sort{i}_prob'],
                                                                     noc_skew_fea[f'hotspot_first_sort{i}_prob'])
            skew_fea[f'hotspot_skew_1_sort{i}_prob'] = add_prob_lists(cwi_skew_fea[f'hotspot_skew_1_sort{i}_prob'],
                                                                      noc_skew_fea[f'hotspot_skew_1_sort{i}_prob'])
            skew_fea[f'hotspot_skew_2_sort{i}_prob'] = add_prob_lists(cwi_skew_fea[f'hotspot_skew_2_sort{i}_prob'],
                                                                      noc_skew_fea[f'hotspot_skew_2_sort{i}_prob'])
            skew_fea[f'gentle_zipf_skew_sort{i}_prob'] = add_prob_lists(cwi_skew_fea[f'gentle_zipf_skew_sort{i}_prob'],
                                                                        noc_skew_fea[f'gentle_zipf_skew_sort{i}_prob'])

        core_config = {}
        core_config['cnt_fea'] = cnt_fea
        core_config['null_fea'] = null_fea
        core_config['sort_fea'] = sort_fea
        core_config['width_fea'] = width_fea
        core_config['continuous_repeat_fea'] = continuous_repeat_fea
        core_config['skew_fea'] = skew_fea

        # write to file
        os.makedirs('./core_config', exist_ok=True)
        config_path = './core_config/core_{}.json'.format(dtype)
        with open(config_path, 'w') as f:
            json.dump(core_config, f, cls=multi_config.NpEncoder, indent=4)

    print('Core-workloads Config generating done!')