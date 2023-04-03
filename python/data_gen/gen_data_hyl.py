import string
import random
import time
import pyarrow.orc as po
import pyarrow as pa
import pyarrow.parquet as pp
from pyarrow import csv
import pandas as pd
import numpy as np
import os
from scripts.utils import *

TOT_NUM = 2000000  # The number of records in generated file
CARDINALITY_INDUCED = 0.2  # Inducing para, not the final actual cardinality_ratio

SKEW = 0.5   # The data records include the uniform part and the skewd part with the ratio of 0.5
SKEWED_LIST = [0.7, 0.1, 0.1, 0.05, 0.025, 0.0125, 0.00625,
               0.00625]  # the value-repeated-frequency of skewd data
SORTED_WINDOW_SIZE = 512
UNSORTED_INDUCED = 0.3

MAX_INT = 30000000
MAX_FLOAT = 30000000
MAX_LENGTH = 50  # for string

print_list = False


def calc_cardinality(df):
    res = len(df[0].unique()) / len(df)
    return res


def calc_most_repeated(df):
    repeat_vals = df[0].value_counts(normalize=True, sort=True)
    # print(repeat_vals)
    res = repeat_vals.values[0]
    return res


def calc_sortedness(np_array):
    np_len = len(np_array)
    corr_list = []
    for i in range(0, np_len, SORTED_WINDOW_SIZE):
        tmp_array = np_array[i:i + SORTED_WINDOW_SIZE]
        sorted_array = np.sort(tmp_array)
        # cal corr of the two
        corr = np.corrcoef(sorted_array, tmp_array)[0, 1]
        corr_list.append(corr)
    res = np.mean(corr_list)
    return res


def gen_integer_data():
    # Generate the uniform random integer list
    if MAX_INT < CARDINALITY_INDUCED * TOT_NUM:
        print('Error: MAX_INT is too small, which should be larger than: ',
              CARDINALITY_INDUCED * TOT_NUM)
        exit(1)
    else:
        unique_count = int(TOT_NUM * CARDINALITY_INDUCED)
        unique_list = random.sample(range(0, MAX_INT), unique_count)
        rand_list = []
        tmp_len = min(unique_count, int(TOT_NUM * (1 - SKEW)))
        rand_list[0:tmp_len] = unique_list[0:tmp_len]
        for i in range(tmp_len, int(TOT_NUM * (1 - SKEW))):
            rand_list.append(random.choice(unique_list))

    # The rest data is the skewed ones which is mostly repeated
    for r in SKEWED_LIST:
        num = int(TOT_NUM * SKEW * r)
        val = np.random.randint(0, MAX_INT)
        tmp_list = np.full(num, val)
        # append tmp_list to rand_list
        rand_list = np.append(rand_list, tmp_list)

    # shuffle the list, then create window and sort
    np.random.shuffle(rand_list)
    for i in range(0, len(rand_list), SORTED_WINDOW_SIZE):
        tmp_list = np.sort(rand_list[i:i + SORTED_WINDOW_SIZE])
        shuffled_tmp_list = np.random.permutation(tmp_list)
        # Replace some value with shuffled list to disorder the data
        replaced_idx = np.random.choice(range(0, len(tmp_list)), size=int(
            len(tmp_list) * UNSORTED_INDUCED), replace=False)
        for idx in replaced_idx:
            tmp_list[idx] = shuffled_tmp_list[idx]
        rand_list[i:i + SORTED_WINDOW_SIZE] = tmp_list

    if print_list:
        print(rand_list)

    res_df = pd.DataFrame(rand_list)
    res_cardinality = calc_cardinality(res_df)
    res_most_repeated = calc_most_repeated(res_df)
    res_sortedness = calc_sortedness(rand_list)
    print("Cardinality: {}, Most_Reapeated: {}, Sorted: {}.".format(
        res_cardinality, res_most_repeated, res_sortedness))
    res_df.to_csv('/root/data/tmp_proc/integer.csv', index=False)
    return res_cardinality, res_most_repeated, res_sortedness


def gen_float_data():
    # Generate the uniform random integer list
    unique_count = int(TOT_NUM * CARDINALITY_INDUCED)
    unique_list = np.random.uniform(0, MAX_FLOAT, unique_count)
    rand_list = []
    tmp_len = min(unique_count, int(TOT_NUM * (1 - SKEW)))
    rand_list[0:tmp_len] = unique_list[0:tmp_len]
    for i in range(tmp_len, int(TOT_NUM * (1 - SKEW))):
        rand_list.append(random.choice(unique_list))

    # The rest data is the skewed ones which is mostly repeated
    for r in SKEWED_LIST:
        num = int(TOT_NUM * SKEW * r)
        tmp_list = []
        tmp_list.append(random.choice(unique_list))
        tmp_list = tmp_list * num
        # append tmp_list to rand_list
        rand_list = np.append(rand_list, tmp_list)

    # shuffle the list, then create window and sort
    np.random.shuffle(rand_list)
    for i in range(0, len(rand_list), SORTED_WINDOW_SIZE):
        tmp_list = np.sort(rand_list[i:i + SORTED_WINDOW_SIZE])
        shuffled_tmp_list = np.random.permutation(tmp_list)
        # Replace some value with shuffled list to disorder the data
        replaced_idx = np.random.choice(range(0, len(tmp_list)), size=int(
            len(tmp_list) * UNSORTED_INDUCED), replace=False)
        for idx in replaced_idx:
            tmp_list[idx] = shuffled_tmp_list[idx]
        rand_list[i:i + SORTED_WINDOW_SIZE] = tmp_list

    if print_list:
        print(rand_list)

    res_df = pd.DataFrame(rand_list)
    res_cardinality = calc_cardinality(res_df)
    res_most_repeated = calc_most_repeated(res_df)
    res_sortedness = calc_sortedness(rand_list)
    print("Cardinality: {}, Most_Reapeated: {}, Sorted: {}.".format(
        res_cardinality, res_most_repeated, res_sortedness))
    res_df.to_csv('/root/data/tmp_proc/float.csv', index=False)
    return res_cardinality, res_most_repeated, res_sortedness


def gen_string_data():
    # Generate the uniform random string list
    unique_count = int(TOT_NUM * CARDINALITY_INDUCED)
    unique_list = []
    for i in range(unique_count):
        rand_str = ''.join(random.sample(string.ascii_letters * 100,
                           np.random.randint(1, MAX_LENGTH)))  # *10 to get repeatd letter
        unique_list.append(rand_str)

    rand_list = []
    tmp_len = min(unique_count, int(TOT_NUM * (1 - SKEW)))
    rand_list[0:tmp_len] = unique_list[0:tmp_len]
    for i in range(tmp_len, int(TOT_NUM * (1 - SKEW))):
        rand_list.append(random.choice(unique_list))

    # The rest data is the skewed ones which is mostly repeated
    for r in SKEWED_LIST:
        num = int(TOT_NUM * SKEW * r)
        tmp_list = []
        tmp_list.append(random.choice(unique_list))
        tmp_list = tmp_list * num
        # append tmp_list to rand_list
        rand_list = rand_list + tmp_list

    # shuffle the list, then create window and sort
    random.shuffle(rand_list)
    for i in range(0, len(rand_list), SORTED_WINDOW_SIZE):
        tmp_list = sorted(rand_list[i:i + SORTED_WINDOW_SIZE])
        shuffled_tmp_list = tmp_list.copy()
        random.shuffle(shuffled_tmp_list)
        # Replace some value with shuffled list to disorder the data
        replaced_idx = np.random.choice(range(0, len(tmp_list)), size=int(
            len(tmp_list) * UNSORTED_INDUCED), replace=False)
        for idx in replaced_idx:
            tmp_list[idx] = shuffled_tmp_list[idx]
        rand_list[i:i + SORTED_WINDOW_SIZE] = tmp_list

    if (print_list):
        print(rand_list)
    res_df = pd.DataFrame(rand_list)
    # use the first 1w records to cal the caridinality_ratio
    cal_card = calc_cardinality(res_df[:10000])
    res_cardinality = calc_cardinality(res_df)
    res_most_repeated = calc_most_repeated(res_df)
    print("Cardinality: {}, Most_Reapeated: {}, Sorted_Induced: {}.".format(
        res_cardinality, res_most_repeated, 1 - UNSORTED_INDUCED))
    res_df.to_csv('/root/data/tmp_proc/string.csv', index=False, header=None)
    return cal_card, res_cardinality, res_most_repeated, (1 - UNSORTED_INDUCED)


if __name__ == '__main__':

    # Generate String data
    cardinality_list = [0.0001, 0.001, 0.01, 0.1, 0.5, 0.9]
    sortedness_list = [0.2, 0.6, 0.9]
    max_length_list = [20, 50, 100, 200]
    skew_list = [0, 0.5, 0.9]
    for length in max_length_list:
        MAX_LENGTH = length
        for car in cardinality_list:
            CARDINALITY_INDUCED = car
            for skewd in skew_list:
                SKEW = skewd
                for sortedness in sortedness_list:
                    UNSORTED_INDUCED = sortedness
                    print("Generating String data...")
                    begin = time.time()
                    cal_card, res_cardinality, res_most_repeated, res_sortedness = gen_string_data()
                    print("String data generated in {} seconds.".format(
                        time.time() - begin))

                    outputs = {}
                    outputs['dtype'] = 'string'
                    outputs['max_len'] = MAX_LENGTH
                    outputs['skew'] = SKEW
                    outputs['cardinality'] = res_cardinality
                    outputs['cal_card'] = cal_card
                    outputs['most_repeated'] = res_most_repeated
                    outputs['sortedness'] = res_sortedness

                    tmp_csv_name = '/root/data/tmp_proc/string.csv'

                    # write parquet and orc files
                    orc_config = {
                        'dictionary_key_size_threshold': 1,
                        'compression': None,
                        'compression_strategy': None,
                        'stripe_size': 33554432,
                        'compression_block_size': 1048576
                    }
                    parquet_config = {
                        'version': '2.6',
                        'use_dictionary': True,
                        'compression': 'None',
                        'compression_level': None,
                        'row_group_size': 524288,
                        'data_page_size': 1048576
                    }
                    try:
                        pa_tbl = csv.read_csv(tmp_csv_name, read_options=csv.ReadOptions(
                        ), parse_options=csv.ParseOptions(delimiter=','))
                        target_pqt = os.path.join(
                            '/root/data/tmp_proc', 'string.parquet')
                        # os.system('/root/codes/format_tuning/cpp/build/pq_writer {} {} {} {} {} {}'.format(
                        #    'string', 524, 102400, 0, 0, 1))
                        pp.write_table(pa_tbl, target_pqt, **parquet_config)

                        target_orc = os.path.join(
                            '/root/data/tmp_proc', 'string.orc')
                        po.write_table(pa_tbl, target_orc, **orc_config)

                        pqt_file_size = os.path.getsize(
                            target_pqt) / 1024 / 1024
                        orc_file_size = os.path.getsize(
                            target_orc) / 1024 / 1024
                        raw_size = os.path.getsize(tmp_csv_name) / 1024 / 1024
                        outputs['pqt_dict_size'] = pqt_file_size
                        outputs['orc_dict_size'] = orc_file_size
                        outputs['raw_size'] = raw_size
                        parse_output(outputs)
                    except Exception as e:
                        print(e)
                    os.system('rm /root/data/tmp_proc/*')

    collect_results()
    print("Exp Done")
