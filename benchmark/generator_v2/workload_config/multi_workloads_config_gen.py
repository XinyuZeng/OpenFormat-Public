# unique num and cardinality analysis
from enum import unique
import json
from unicodedata import category
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os

stats_file_name = '../../feature/outputs/stats.csv'


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


def skewness_anlaysis(_df, dtype):

    df = _df[_df['dtype'] == dtype]

    # null data
    null_df = df[df['unique_num'] == 0]

    # single data
    single_df = df[df['unique_num'] == 1]

    # binary data
    bi_df = df[df['unique_num'] == 2]

    # uniform data
    uni_df = df[(df['unique_num'] >= 3) & (df['zipf'] < 0.01)]

    # real hotspot
    real_hotspot_df = df[(df['unique_num'] >= 3) & (df['most_repeated_0'] > 9 * df['most_repeated_2'])]

    # zipf data
    zipf_df = df[(df['unique_num'] >= 3) & (df['zipf'] >= 0.01) & (df['zipf'] <= 2)]

    # statistically hotspot (real_hotspot + zipf )
    stat_hotspot_df = df[(df['unique_num'] >= 3) & (df['zipf'] >= 0.01)]

    tot_col_num = len(df)
    if tot_col_num != 0:
        pattern_prob = pd.DataFrame({
            'null': [len(null_df) / tot_col_num],
            'single': [len(single_df) / tot_col_num],
            'binary': [len(bi_df) / tot_col_num],
            'uniform': [len(uni_df) / tot_col_num],
            'real_hotspot': [len(real_hotspot_df) / tot_col_num],
            'gentle_zipf': [(tot_col_num - len(null_df) - len(single_df) - len(bi_df) - len(uni_df) - len(real_hotspot_df)) / tot_col_num]
        })
    else:
        # print("Skewness function warning, No corresponding data of type: ", dtype)
        pattern_prob = pd.DataFrame({'null': [0], 'single': [0], 'binary': [0], 'uniform': [0], 'real_hotspot': [0], 'gentle_zipf': [0]})

    # binary skew
    bi_cut = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
    bi_skew = bi_df['most_repeated_0']
    bi_hist = pd.cut(bi_skew, bi_cut).value_counts(normalize=True).sort_index()

    # real hotspot skew
    hotspot_first_cut = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    hotspot_skew_cut = [1, 2, 3, 4, 5, 8, 10, 20, 50, 100, 1000, 10000]
    hotspot_first = stat_hotspot_df['most_repeated_0']
    hotspot_skew1 = stat_hotspot_df['most_repeated_0'] / (stat_hotspot_df['most_repeated_1'] + 1e-6)
    hotspot_skew2 = stat_hotspot_df['most_repeated_0'] / (stat_hotspot_df['most_repeated_2'] + 1e-6)
    hotspot_first_hist = pd.cut(hotspot_first, hotspot_first_cut).value_counts(normalize=True).sort_index()
    hotspot_skew1_hist = pd.cut(hotspot_skew1, hotspot_skew_cut).value_counts(normalize=True).sort_index()
    hotspot_skew2_hist = pd.cut(hotspot_skew2, hotspot_skew_cut).value_counts(normalize=True).sort_index()

    # zipf distribution
    zipf_cut = list([0.01, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0])
    zipf_skew = zipf_df['zipf']
    zipf_hist = pd.cut(zipf_skew, zipf_cut).value_counts(normalize=True).sort_index()

    res_dict = {}
    res_dict['skew_pattern_prob'] = np.array(pattern_prob.loc[0, :])
    res_dict['bi_skew'] = bi_cut
    res_dict['bi_skew_prob'] = bi_hist.values
    res_dict['hotspot_first'] = hotspot_first_cut
    res_dict['hotspot_first_prob'] = hotspot_first_hist.values
    res_dict['hotspot_skew'] = hotspot_skew_cut
    res_dict['hotspot_skew_1_prob'] = hotspot_skew1_hist.values
    res_dict['hotspot_skew_2_prob'] = hotspot_skew2_hist.values
    res_dict['gentle_zipf_skew'] = zipf_cut
    res_dict['gentle_zipf_skew_prob'] = zipf_hist.values

    return res_dict


def old_continuous_repeat(_df, dtype):
    df = _df[_df['dtype'] == dtype]
    repeat_block_cut = [0, 1, 2, 3, 5, 10, 20, 50, 100, 200, 300, 400, 512]
    repeat_block_cnt_ratio_cut = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
    next_block_skew_ratio_cut = [0, 0.01, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]

    most_repeat_block_list = []
    most_repeat_block_cnt_ratio_list = []
    next_block_skew_ratio_list = []  # the ratio of next block / the sum ratio of all the rest blocks
    for row in df.iterrows():
        row_content = row[1]
        repeat_num_ratio_list = []
        for idx in range(len(repeat_block_cut) - 1):
            tmp_ratio = row_content[f'({repeat_block_cut[idx]}, {repeat_block_cut[idx+1]}]_repeated']
            repeat_num_ratio_list.append(tmp_ratio)
        # get the idx of repeat_block which appears most (ratio is the largest)
        most_repeat_block_idx = np.argmax(repeat_num_ratio_list)
        most_repeat_block_list.append(repeat_block_cut[most_repeat_block_idx] + 1)
        most_repeat_block_cnt_ratio_list.append(repeat_num_ratio_list[most_repeat_block_idx])
        # get the next_right_block idx, if most_right, get the left block
        next_block_idx = (most_repeat_block_idx +
                          1) if most_repeat_block_idx < len(repeat_num_ratio_list) - 1 else len(repeat_num_ratio_list) - 2
        next_block_skew_ratio = repeat_num_ratio_list[next_block_idx] / (1 - repeat_num_ratio_list[most_repeat_block_idx] + 1e-6)
        next_block_skew_ratio_list.append(next_block_skew_ratio)
    # cal the distribution of most repeat block and its average appearing ratio
    print(most_repeat_block_list)
    most_repeat_block_hist = pd.cut(pd.Series(most_repeat_block_list, dtype='int64'),
                                    repeat_block_cut).value_counts(normalize=True).sort_index()
    most_repeat_block_cnt_ratio_hist = pd.cut(pd.Series(most_repeat_block_cnt_ratio_list, dtype='float64'),
                                              repeat_block_cnt_ratio_cut).value_counts(normalize=True).sort_index()
    # cal the distribution of next block skew ratio
    next_block_skew_ratio_hist = pd.cut(pd.Series(next_block_skew_ratio_list, dtype='float64'),
                                        next_block_skew_ratio_cut).value_counts(normalize=True).sort_index()

    res_dict = {}
    res_dict['repeat_num_cut'] = repeat_block_cut
    res_dict['repeat_num_most_prob'] = list(most_repeat_block_hist.values.round(4))
    res_dict['repeat_num_ratio_cut'] = repeat_block_cnt_ratio_cut
    res_dict['repeat_num_ratio_most_prob'] = list(most_repeat_block_cnt_ratio_hist.values.round(4))
    res_dict['next_block_skew_ratio_cut'] = next_block_skew_ratio_cut
    res_dict['next_block_skew_ratio_prob'] = list(next_block_skew_ratio_hist.values.round(4))

    return res_dict


def continuous_repeat(_df, dtype):
    continue_mean_df = _df[_df['dtype'] == dtype]['continue_mean']

    # continue_mean_cut = [0, 5, 10, 25, 50, 100, 200, 300, 400, 512]
    continue_mean_cut = [0, 4, 8, 16, 32, 64, 128, 256, 384, 512]
    repeat_block_cut = [0, 1, 2, 3, 5, 10, 20, 50, 100, 200, 300, 400, 512]
    # when the continue_mean is [0,4] the repeat_block_ratio is [0.98,0.01,...]
    continue_mean_map_repeat_block_ratio = [
        [0.98, 0.01, 0.01, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],  # (0,4]
        [0.8, 0.1, 0.04, 0.03, 0.03, 0, 0, 0, 0, 0, 0, 0],  # (4,8]
        [0.75, 0.18, 0.03, 0.02, 0.01, 0.01, 0, 0, 0, 0, 0, 0],  # (8,16]
        [0.7, 0.15, 0.04, 0.03, 0.03, 0.03, 0.02, 0, 0, 0, 0, 0],  # (16,32]
        [0.65, 0.14, 0.05, 0.05, 0.05, 0.03, 0.02, 0.01, 0, 0, 0, 0],  # (32,64]
        [0.58, 0.10, 0.04, 0.05, 0.08, 0.06, 0.05, 0.02, 0.01, 0, 0, 0],  # (64,128]
        [0.4, 0.12, 0.04, 0.04, 0.03, 0.02, 0.04, 0.1, 0.11, 0.07, 0.02, 0.01],  # (128,256]
        [0.30, 0.14, 0.08, 0.1, 0.13, 0.09, 0.06, 0.02, 0.01, 0.01, 0.01, 0.04],  # (256,384]
        [0.01, 0, 0, 0, 0, 0, 0, 0, 0, 0.01, 0.01, 0.97]  # (384,512]
    ]

    continue_mean_hist = pd.cut(continue_mean_df, continue_mean_cut).value_counts(normalize=True).sort_index()

    res_dict = {}
    res_dict['continue_mean_cut'] = continue_mean_cut
    res_dict['continue_mean_prob'] = list(continue_mean_hist.values.round(4))
    res_dict['repeat_num_cut'] = repeat_block_cut
    res_dict['continue_mean_map_repeat_block_ratio'] = continue_mean_map_repeat_block_ratio

    return res_dict


def feature_analysis(_df):
    unique_cut = [2, 10, 100, 1000]  # from 3 to 1000
    car_cut = [0, 1e-8, 1e-7, 1e-6, 1e-5, 1e-4, 1e-3, 1e-2, 1e-1, 1]
    null_cut = [0, 1e-8, 1e-7, 1e-6, 1e-5, 1e-4, 1e-3, 1e-2, 1e-1, 0.5, 1]
    sort_cut = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
    width_cut_dict = {
        'float64': [0, 100, 10000, 1000000, 100000000],
        'bool': [0, 1],
        'int64': [0, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000],
        # 'object': [0, 5, 10, 25, 50, 100, 250, 500, 1000]
        'object': [0, 5, 10, 25, 50, 75, 100, 150, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1500, 2000, 2500, 3000]
    }
    width_skew_cut = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]

    dtype_list = ['float64', 'object', 'int64']
    dtype_map_config_dict = {}
    for dtype in dtype_list:
        # count the stats for each column
        sort_scores = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.01]  # sort<0.1 is sort0; 0.1<=sort<0.2 is sort1
        car_ratios = [0, 1e-8, 1e-7, 1e-6, 1e-5, 1e-4, 1e-3, 1e-2, 1e-1, 1.01]
        category_df_sort_list = []
        unique_df_sort_list = []
        car_df_sort_list = []
        for i in range(len(sort_scores) - 1):
            tmp_category = _df[(_df['dtype'] == dtype) & (_df['unique_num'] <= 1000) & (_df['unique_num'] >= 3)
                               & (_df['sortedness_mean'] >= sort_scores[i]) & (_df['sortedness_mean'] < sort_scores[i + 1])]
            tmp_unique = _df[(_df['dtype'] == dtype) & (_df['sortedness_mean'] >= sort_scores[i]) &
                             (_df['sortedness_mean'] < sort_scores[i + 1])]['unique_num']
            tmp_car = _df[(_df['dtype'] == dtype) & (_df['sortedness_mean'] >= sort_scores[i]) &
                          (_df['sortedness_mean'] < sort_scores[i + 1])]['cardinality']
            category_df_sort_list.append(tmp_category)
            unique_df_sort_list.append(tmp_unique)
            car_df_sort_list.append(tmp_car)
        null_df = _df[_df['dtype'] == dtype]['null_ratio']
        # TODO: there are full-null columns in sort_df
        sort_df = _df[_df['dtype'] == dtype]['sortedness_mean']
        width_df_car_list = []
        for i in range(len(car_ratios) - 1):
            tmp_width = _df[(_df['dtype'] == dtype) & (_df['cardinality'] >= car_ratios[i]) &
                            (_df['cardinality'] < car_ratios[i + 1])]['width_mean']
            width_df_car_list.append(tmp_width)
        width_skew_df = _df[_df['dtype'] == dtype]['width_most_repeated_0']
        # cut the distribution
        null_hist = pd.cut(null_df, null_cut, include_lowest=True).value_counts(normalize=True).sort_index()
        sort_hist = pd.cut(sort_df, sort_cut).value_counts(normalize=True).sort_index()
        width_skew_hist = pd.cut(width_skew_df, width_skew_cut, include_lowest=True).value_counts(normalize=True).sort_index()

        # write config to dict
        cnt_fea, null_fea, sort_fea, width_fea = {}, {}, {}, {}
        cnt_fea['unique_num'] = unique_cut
        cnt_fea['cardinality'] = car_cut
        for i in range(len(sort_scores) - 1):
            cate_ratio = len(category_df_sort_list[i]) / len(_df[(_df['dtype'] == dtype)])
            unique_hist = pd.cut(unique_df_sort_list[i], unique_cut).value_counts(normalize=True).sort_index()
            car_hist = pd.cut(car_df_sort_list[i], car_cut).value_counts(normalize=True).sort_index()
            cnt_fea[f'is_categorical_sort{i}'] = cate_ratio  # unique_num < 1000
            cnt_fea[f'unique_num_sort{i}_prob'] = list(unique_hist.values.round(4))
            cnt_fea[f'cardinality_sort{i}_prob'] = list(car_hist.values.round(4))
        null_fea['null_ratio'] = null_cut
        null_fea['null_ratio_prob'] = list(null_hist.values.round(4))
        sort_fea['sort_window'] = 512  # default
        sort_fea['sort_score'] = sort_cut
        sort_fea['sort_score_prob'] = list(sort_hist.values.round(4))
        width_fea['width_center'] = width_cut_dict[dtype]
        for i in range(len(car_ratios) - 1):
            width_hist = pd.cut(width_df_car_list[i], width_cut_dict[dtype]).value_counts(normalize=True).sort_index()
            tmp_prob_list = list(width_hist.values.round(4))
            # TODO: brute force now
            if np.isnan(tmp_prob_list[0]):
                tmp_prob_list = [1 / len(width_hist)] * len(tmp_prob_list)
            width_fea[f'width_center_car{i}_prob'] = tmp_prob_list
        width_fea['width_skew'] = width_skew_cut
        width_fea['width_skew_prob'] = list(width_skew_hist.values.round(4))

        # add continuous repeat feature
        continuous_repeat_fea = {}
        for i in range(len(sort_scores) - 1):
            repeat_dict = continuous_repeat(_df[(_df['sortedness_mean'] >= sort_scores[i]) & (_df['sortedness_mean'] < sort_scores[i + 1])],
                                            dtype)
            # log the cut list only once
            if i == 0:
                continuous_repeat_fea['continuous_mean_cut'] = repeat_dict['continue_mean_cut']
                continuous_repeat_fea['repeat_num_cut'] = repeat_dict['repeat_num_cut']
                continuous_repeat_fea['continue_mean_map_repeat_block_ratio'] = repeat_dict['continue_mean_map_repeat_block_ratio']
            continuous_repeat_fea[f'continuous_mean_sort{i}_prob'] = repeat_dict['continue_mean_prob']

        # write to json
        config_dict = {}
        config_dict['cnt_fea'] = cnt_fea
        config_dict['null_fea'] = null_fea
        config_dict['sort_fea'] = sort_fea
        config_dict['width_fea'] = width_fea
        config_dict['continuous_repeat_fea'] = continuous_repeat_fea

        # deal with skewness of low sort score
        skew_fea = {}
        skew_fea['skew_pattern'] = ["null", "single", "binary", "uniform", "hotspot", "gentle_zipf"]
        for i in range(len(sort_scores) - 1):
            skew_dict = skewness_anlaysis(_df[(_df['sortedness_mean'] >= sort_scores[i]) & (_df['sortedness_mean'] < sort_scores[i + 1])],
                                          dtype)
            if i == 0:
                skew_fea['bi_skew'] = skew_dict['bi_skew']
                skew_fea['hotspot_first'] = skew_dict['hotspot_first']
                skew_fea['hotspot_skew'] = skew_dict['hotspot_skew']
                skew_fea['gentle_zipf_skew'] = skew_dict['gentle_zipf_skew']
            skew_fea[f'skew_pattern_sort{i}_prob'] = list(skew_dict['skew_pattern_prob'].round(4))
            skew_fea[f'bi_skew_sort{i}_prob'] = list((skew_dict['bi_skew_prob']).round(4))
            skew_fea[f'hotspot_first_sort{i}_prob'] = list((skew_dict['hotspot_first_prob']).round(4))
            skew_fea[f'hotspot_skew_1_sort{i}_prob'] = list((skew_dict['hotspot_skew_1_prob']).round(4))
            skew_fea[f'hotspot_skew_2_sort{i}_prob'] = list((skew_dict['hotspot_skew_2_prob']).round(4))
            skew_fea[f'gentle_zipf_skew_sort{i}_prob'] = list((skew_dict['gentle_zipf_skew_prob']).round(4))
        config_dict['skew_fea'] = skew_fea

        dtype_map_config_dict[dtype] = config_dict
    return dtype_map_config_dict


if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    df = pd.read_csv(stats_file_name, sep=',')
    workload_list = ['classic', 'geo', 'log', 'ml', 'bi']
    workload_dir_dict = {
        'classic': ['imdb', 'yelp', 'UKPP', 'menu'],
        'geo': ['cells', 'geo', 'flight'],
        'log': ['edgar', 'mgbench'],
        'ml': ['ml'],
        'bi': ['cwi']
    }
    for workload in workload_list:
        os.makedirs('./{}_config'.format(workload), exist_ok=True)
        dir = workload_dir_dict[workload]
        target_df = df[df['dataset'].isin(dir)]
        dtype_map_config_dict = feature_analysis(target_df)
        for dtype in dtype_map_config_dict.keys():
            config_dict = dtype_map_config_dict[dtype]
            config_path = './{}_config/{}_{}.json'.format(workload, workload, dtype)
            with open(config_path, 'w+') as f:
                json.dump(config_dict, f, cls=NpEncoder, indent=4)

    print('Multiple-workloads config generating done!')