from math import isnan
import pandas as pd
import numpy as np
# from LZ77 import LZ77Compressor
import os
from utils import *
import time
from scipy.optimize import curve_fit
import matplotlib.pyplot as plt
import datetime


def calc_cardinality(df, stats):
    begin_time = time.time()
    stats['nrows'] = len(df)
    df = df[~df.isnull()]
    res = len(df.unique()) / len(df) if len(df) != 0 else 0
    stats['cardinality'] = res
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
    df = df[~df.isnull()]
    if len(df) == 0:
        df = pd.Series([0])
    # if int
    try:
        if df.dtypes == 'Int64':
            mean_val = df.mean()
            max_val = df.max()
            median_val = df.median()
            min_val = df.min()
    except:
        if df.dtypes == 'int64':  # store as int64 in pandas
            mean_val = df.mean()
            max_val = df.max()
            median_val = df.median()
            min_val = df.min()
        elif df.dtypes == 'object':  # store as binary in parquet
            # print('df_content:{}'.format(df[:5]))
            len_df = df.astype(str).str.len()
            # print('len_df_content:{}'.format(len_df[:5]))
            mean_val = len_df.mean()
            max_val = len_df.max()
            median_val = len_df.median()
            min_val = len_df.min()
        elif df.dtypes == 'float64':  # store as double in parquet
            mean_val = 8
            max_val = 8
            median_val = 8
            min_val = 8
        elif df.dtypes == 'bool':
            mean_val = 0.125
            max_val = 0.125
            median_val = 0.125
            min_val = 0.125
        else:
            print("unknown type: {}".format(df.dtypes))
    stats['width_mean'] = float(mean_val)
    stats['width_min'] = float(min_val)
    stats['width_max'] = float(max_val)
    stats['width_median'] = float(median_val)
    return 1


def cal_sortedness(df, stats, window=512):
    if df.dtypes == 'object':
        df = df.astype(str)
    df = df[~df.isnull()].reset_index(drop=True)
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
                # if pd.isnull(row[1]):
                #     continue
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
    if len(res) == 0:
        res = pd.DataFrame([0])
    stats['sortedness_max'] = float(res.max()[0])
    stats['sortedness_mean'] = float(res.mean()[0])
    stats['sortedness_min'] = float(res.min()[0])
    # stats['sortedness_std'] = float(res.std()[0])
    return 1


def calc_most_repeated(df, stats, num):
    # begin_time = time.time()
    repeat_vals = df.value_counts(normalize=True, sort=True)
    stats['unique_num'] = len(repeat_vals.values)
    res = repeat_vals.values[:num]
    if len(res) < num:
        res = np.append(res, np.zeros(num - len(res)))
    for i in range(num):
        stats['most_repeated_{}'.format(i)] = float(res[i])
    # stats['most_repeated_time'] = time.time() - begin_time
    return 1


def calc_continue_repeat(df, stats, window=512):
    begin_time = time.time()
    df = df[~df.isnull()].reset_index(drop=True)
    repeat_list = []
    for i in range(0, len(df), window):
        repeat_num = 0
        max_repeat_num = 0
        repeat_val = None
        tmp_df = df[i:i + window]
        for row in tmp_df.iteritems():
            # if pd.isnull(row[1]):
            #     continue
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
    if len(res) == 0:
        res = pd.DataFrame([0])
    stats['continue_mean'] = float(res.mean()[0])
    #stats['continue_std'] = float(res.std()[0])
    stats['continue_max'] = float(res.max()[0])
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
        stats['zipf'] = 0
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
    stats['zipf'] = popt[0]
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
    df = df[~df.isnull()]
    if len(df) == 0:
        df = pd.Series([0, 0, 0])
    try:
        if df.dtypes == 'Int64':
            width_df = pd.cut(df, [-1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 100000000000])
            hist_df = width_df.value_counts(normalize=True, sort=True)
    except:
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


def cal_repetead_block(df, stats):
    repeat_num_cut = [0, 1, 2, 3, 5, 10, 20, 50, 100, 200, 300, 400, 512]
    repeat_num_list = []
    repeat_num = 0
    repeat_val = None
    df = df[~df.isnull()].reset_index(drop=True)
    if len(df) == 0:
        df = pd.Series([0, 0, 0])
        repeat_num_list = [1]
    for row in df.iteritems():
        # if pd.isnull(row[1]):
        #     continue
        if row[1] == repeat_val:
            repeat_num += 1
            if repeat_num >= 512:
                repeat_num_list.append(512)
                repeat_num = 0
        else:
            # log and update the repeat_num
            repeat_num_list.append(repeat_num)
            repeat_num = 1
            repeat_val = row[1]
    repeat_num_hist = pd.cut(pd.Series(repeat_num_list), repeat_num_cut).value_counts(normalize=True).sort_index(ascending=False)
    for repeat_num in repeat_num_hist.index:
        stats[f'{repeat_num}_repeated'] = repeat_num_hist[repeat_num]


if __name__ == '__main__':
    # print("ERROR: You are not supposed to run this program unless you have prepared the datasets.")
    # exit(1)
    root_dir = '/root/data'
    dir_lists = os.listdir(root_dir)
    for dir in dir_lists:
        dir_name = os.path.join(root_dir, dir)
        file_lists = os.listdir(dir_name)
        print(file_lists)
        for base_file_name in file_lists[::]:
            csv_file_name = os.path.join(dir_name, base_file_name)
            if dir in ['edgar', 'ml', 'yelp', 'flight', 'cells', 'menu', 'mgbench', "UKPP"]:
                df = pd.read_csv(csv_file_name, sep=',', header=None, skiprows=1, on_bad_lines='warn', low_memory=False)
            elif dir in ['imdb', 'geo']:
                df = pd.read_csv(csv_file_name, sep='\t', header=None, skiprows=1, on_bad_lines='warn', low_memory=False)
            elif dir in ['cwi']:
                df = pd.read_csv(csv_file_name, sep='|', header=None, skiprows=1, on_bad_lines='warn', low_memory=False)

            col_num = len(df.columns)
            print("col_num: ", col_num)
            for col_idx in range(0, col_num):
                output_stats = {}
                origin_df = df[col_idx]
                if origin_df.dtypes == 'float64':
                    try:
                        tmp_df = origin_df.astype('Int64')
                    except:
                        tmp_df = origin_df
                else:
                    tmp_df = origin_df
                output_stats['dataset'] = dir_name.split(r'/')[-1]
                output_stats['file_name'] = base_file_name
                output_stats['col_idx'] = col_idx
                dtype_str = str(tmp_df.dtypes)
                output_stats['dtype'] = dtype_str if dtype_str != 'Int64' else 'int64'

                calc_cardinality(tmp_df, output_stats)
                calc_null_ratio(tmp_df, output_stats)
                cal_sortedness(tmp_df, output_stats)
                calc_most_repeated(tmp_df, output_stats, 3)
                calc_continue_repeat(tmp_df, output_stats)
                value_width(tmp_df, output_stats)
                cal_width_distribution(tmp_df, output_stats)
                cal_value_zipf(tmp_df, output_stats)
                cal_repetead_block(tmp_df, output_stats)

                parse_output(output_stats)
            print(datetime.datetime.now(), "Finish file: {}".format(base_file_name))
        print("Finish dir: {}".format(dir_name))
    collect_results()
