import json
import pandas as pd
import numpy as np
import os
import time
import sys
from math import isnan
from scipy.optimize import curve_fit
import matplotlib.pyplot as plt

root_dir = './core_r10m_c20_new'


def parse_output(output_stats):
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    os.makedirs(os.path.dirname(os.path.join(root_dir, "outputs/stats.json")), exist_ok=True)
    stats = open(os.path.join(root_dir, "outputs/stats.json"), 'a+')
    stats.write(json.dumps(output_stats) + "\n")
    stats.close()


def collect_results():
    data = []
    filename = "outputs/stats.json"
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    os.makedirs(os.path.dirname(os.path.join(root_dir, filename)), exist_ok=True)
    f = open(os.path.join(root_dir, filename))
    for line in f:
        data.append(json.loads(line.strip()))
    df = pd.DataFrame(data=data)
    df.to_csv(os.path.join(root_dir, "outputs/stats.csv"), index=False)
    os.system("rm " + os.path.join(root_dir, "outputs/stats.json"))


def calc_cardinality(df, stats):
    begin_time = time.time()
    df = df[~df.isnull()]
    unique_num = len(df.unique())
    cardinality = unique_num / len(df) if len(df) != 0 else 0
    stats['cardinality'] = cardinality
    stats['unique_num'] = unique_num
    # stats['cardinality_time'] = time.time() - begin_time
    return 1


def calc_null_ratio(df, stats):
    begin_time = time.time()
    # if df.dtypes == 'int64':
    #     res = (df == 0).sum() / len(df)
    # elif df.dtypes == 'bool':
    #     res = (df == False).sum() / len(df)
    res = df.isnull().sum() / len(df)
    stats['null_ratio'] = res
    # stats['null_ratio_time'] = time.time() - begin_time
    return 1


def value_width(df, stats):
    begin_time = time.time()
    # if int
    if df.dtypes == 'int64':  # store as int64 in pandas
        # FIXME : just use the value not the bit-width
        mean_val = df.mean()
        std_val = df.std()
        max_val = df.max()
        median_val = df.median()
        quantile_val = df.quantile()
        min_val = df.min()
    elif df.dtypes == 'object':  # store as binary in parquet
        # print('df_content:{}'.format(df[:5]))
        len_df = df.astype(str).str.len()
        # print('len_df_content:{}'.format(len_df[:5]))
        mean_val = len_df.mean()
        std_val = len_df.std()
        max_val = len_df.max()
        median_val = len_df.median()
        quantile_val = len_df.quantile()
        min_val = len_df.min()
    elif df.dtypes == 'float64':  # store as double in parquet
        mean_val = 8
        std_val = 8
        max_val = 8
        median_val = 8
        quantile_val = 8
        min_val = 8
    elif df.dtypes == 'bool':
        mean_val = 0.125
        std_val = 0.125
        max_val = 0.125
        median_val = 0.125
        quantile_val = 0.125
        min_val = 0.125
    else:
        print("unknown type: {}".format(df.dtypes))
    stats['width_center'] = float(mean_val)
    #stats['width_std'] = float(std_val)
    # stats['width_min'] = float(min_val)
    # stats['width_max'] = float(max_val)
    # stats['width_median'] = float(median_val)
    # stats['width_quantile'] = float(quantile_val)
    return 1


def cal_sortedness(df, stats, window=512):
    if df.dtypes == 'object':
        df = df.astype(str)
    sorted_list = []
    for i in range(1, len(df), window):  # start from 1 to have 'i-1'
        # define larger and equal num to calculate sortedness
        larger_para = 0
        equal_para = 0
        tmp_value = df[i - 1]
        tmp_df = df[i:i + window - 1]
        tmp_len = tmp_df.count() + 1
        for row in tmp_df.iteritems():
            try:
                if pd.isnull(row[1]):
                    continue
                if row[1] > tmp_value:
                    larger_para += 1
                elif row[1] == tmp_value:
                    equal_para += 1
                tmp_value = row[1]  # update tmp_value for following comparison
            except Exception as e:
                print("row: {} ".format(row), "tmp_value: {}".format(tmp_value), e)
        smaller_para = tmp_len - equal_para - larger_para - 1
        # if the sortedn_num is tmp_len/2, then it is quite no-monotonical, so we standardize it
        sorted_para = ((max(smaller_para, larger_para) + equal_para) - int(tmp_len / 2) + 1) / int(
            tmp_len / 2 + 0.5)  # the last 0.5 is for rounding to avoid sortedness > 1
        sorted_list.append(sorted_para)
    res = pd.DataFrame(sorted_list)
    stats['sort_window'] = window
    stats['sort_score'] = float(res.mean()[0])
    # stats['sortedness_max'] = float(res.max()[0])
    # stats['sortedness_mean'] = float(res.mean()[0])
    # stats['sortedness_min'] = float(res.min()[0])
    # stats['sortedness_std'] = float(res.std()[0])
    return 1


def calc_most_repeated(df, stats, num=3):
    # begin_time = time.time()
    repeat_vals = df.value_counts(normalize=True, sort=True)
    res = repeat_vals.values[:num]
    if len(res) < num:
        res = np.append(res, np.zeros(num - len(res)))
    stats['bi_skew'] = float(res[0])
    rank_dict = {0: 'first', 1: 'second', 2: 'third', 3: 'fourth', 4: 'fifth'}
    for i in range(num):
        stats['hotspot_{}'.format(rank_dict[i])] = float(res[i])
    # stats['most_repeated_time'] = time.time() - begin_time
    return 1


def calc_continue_repeat(df, stats, window=512):
    begin_time = time.time()
    repeat_list = []
    for i in range(0, len(df), window):
        repeat_num = 0
        max_repeat_num = 0
        repeat_val = None
        tmp_df = df[i:i + window]
        for row in tmp_df.iteritems():
            if row[1] == repeat_val:
                repeat_num += 1
            else:
                repeat_val = row[1]
                if repeat_num > max_repeat_num:
                    max_repeat_num = repeat_num
                repeat_num = 1
        if repeat_num > max_repeat_num:
            max_repeat_num = repeat_num
        repeat_list.append(max_repeat_num)
    res = pd.DataFrame(repeat_list)
    stats['repeat_continue_mean'] = float(res.mean()[0])
    #stats['continue_std'] = float(res.std()[0])
    # stats['continue_max'] = float(res.max()[0])
    #stats['continue_median'] = float(res.median()[0])
    #stats['continue_quantile'] = float(res.quantile()[0])
    #stats['continue_time'] = time.time() - begin_time
    return 1


def zipf_func(X, a, b):
    k, n = X
    n_int = int(n[0])
    denominator = 0
    for i in range(1, n_int + 1):
        denominator += 1 / i**a
    y = (1 / k**a) / denominator + b
    return y


def cal_value_zipf(df, stats):
    # get and preprocess ydata
    repeat_vals = df.value_counts(normalize=True, sort=True)
    ydata = repeat_vals.values[:]
    if (len(ydata) <= 2):
        stats['zipf_skew'] = 0
        stats['zipf_r2'] = 0
        return
    data_len = len(ydata)
    # get xdata
    xdata1 = np.arange(1, data_len + 1)
    xdata2 = np.full(data_len, data_len)

    # fit and cal r_squared
    popt, pcov = curve_fit(zipf_func, (xdata1, xdata2), ydata, maxfev=5000)
    ss_tot = np.var(ydata)
    ss_res = np.sum((ydata - zipf_func((xdata1, xdata2), *popt))**2)
    r_squared = 1 - (ss_res / ss_tot)
    #print('ss_tot: %5.4f, ss_res: %5.4f, r: %5.4f'%(ss_tot,ss_res,r_squared))

    # log the res
    stats['zipf_skew'] = popt[0]
    stats['zipf_r2'] = r_squared

    # fig = plt.figure(figsize=(8, 6))
    # plt.plot(xdata1, ydata, 'bo-', markersize='5', label='data')
    # plt.plot(xdata1, zipf_func((xdata1, xdata2), *popt), 'r-',
    #          label='zipf_a=%5.3f, zipf_b=%5.3f, r^2=%6.5f' % (popt[0], popt[1], r_squared))
    # plt.legend()
    # plt.show()
    # fig.savefig(
    #     './figs/value_zipf/{}_{}.svg'.format(stats['file_name'], stats['col_idx']))
    # plt.close()
    return 1


def cal_width_distribution(df, stats):
    # get and preprocess ydata
    if df.dtypes == 'int64':
        # have a upper bound of 1e11
        width_df = pd.cut(df, [-1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 100000000000])
        hist_df = width_df.value_counts(normalize=True, sort=True)
    elif df.dtypes == 'object':
        len_df = df.astype(str).str.len()
        width_df = pd.cut(len_df, [0, 5, 10, 25, 50, 100, 250, 500, 1000])
        hist_df = width_df.value_counts(normalize=True, sort=True)
    else:
        hist_df = pd.Series([0, 0, 0, 0, 0, 0, 0, 0, 0])
    for i in range(3):
        stats['width_most_repeated_{}'.format(i)] = float(hist_df.values[i])
    if isnan(stats['width_most_repeated_{}'.format(0)]):
        stats['width_most_repeated_{}'.format(0)] = 1

    return 1


if __name__ == '__main__':
    dir_lists = sys.argv[1:]
    if len(dir_lists) == 0:
        dir_lists = ['gen_data', 'real_data']
    for dir in dir_lists:
        dir_name = os.path.join(root_dir, dir)  # './tmp/gen_data'
        # change to the folder of this python file and check if the dir_name exists
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        if not os.path.exists(dir_name):
            print('Error: dir {} not exists'.format(dir_name))
            continue
        file_lists = os.listdir(dir_name)
        for base_file_name in file_lists[::]:
            if not base_file_name.endswith('.csv'):
                continue
            csv_file_name = os.path.join(dir_name, base_file_name)
            # 👀, Plz pay attention to the separator
            df = pd.read_csv(csv_file_name, sep=',', header=None, on_bad_lines='warn', nrows=1000000, low_memory=False)
            col_num = len(df.columns)
            # print("col_num: ", col_num)
            for col_idx in range(0, col_num):
                output_stats = {}
                tmp_df = df[col_idx]
                output_stats['dataset'] = dir_name.split(r'/')[-1]
                output_stats['file_name'] = base_file_name
                output_stats['col_idx'] = col_idx
                data_type_dict = {'int64': 'int', 'float64': 'float', 'object': 'string'}
                output_stats['data_type'] = data_type_dict[str(tmp_df.dtypes)]
                output_stats['row_num'] = len(tmp_df)

                calc_cardinality(tmp_df, output_stats)
                calc_null_ratio(tmp_df, output_stats)
                cal_sortedness(tmp_df, output_stats)
                calc_continue_repeat(tmp_df, output_stats)
                value_width(tmp_df, output_stats)
                calc_most_repeated(tmp_df, output_stats, 3)
                # cal_width_distribution(tmp_df, output_stats)
                cal_value_zipf(tmp_df, output_stats)

                parse_output(output_stats)
            print("Stats finish file: {}".format(base_file_name))
        print("Stats finish dir: {}".format(dir_name))
    collect_results()
