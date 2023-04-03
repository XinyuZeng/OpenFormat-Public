from cmath import isnan, nan
from math import sqrt
import datetime
import random
import pandas as pd
import numpy as np
import json
import os
import sys
import traceback
from generator import *
import logging

LOG = logging.getLogger(__name__)
LOG_handler = logging.StreamHandler()
LOG_formatter = logging.Formatter(fmt='%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s', datefmt='%m-%d-%Y %H:%M:%S')
LOG_handler.setFormatter(LOG_formatter)
LOG_handler.setLevel(logging.DEBUG)
LOG.addHandler(LOG_handler)
# LOG.setLevel(logging.WARNING)

_NULL_RATIO_SLOT_DICT, _SORT_SCORE_SLOT_DICT = {}, {}
_SORTED_WINDOW_SIZE = 512
_CONTINUE_MEAN_SLOT_SORT_DICTS = []
_CONTINUE_MEAN_CUT, _REPEAT_NUM_CUT, _CONTINUE_MEAN_MAP_REPEAT_BLOCK_RATIO = [], [], []
_SKEW_PATTERN_SLOT_SORT_DICTS, _BI_SKEW_SLOT_SORT_DICTS, _GENTLE_ZIPF_SKEW_SLOT_SORT_DICTS = [], [], []
_HOTSPOT_FIRST_SLOT_SORT_DICTS, _HOTSPOT_SKEW_1_SLOT_SORT_DICTS, _HOTSPOT_SKEW_2_SLOT_SORT_DICTS = [], [], []
_IS_CATEGORICAL_SLOT_SORT_DICTS = []
_UNIQUE_NUM_SLOT_SORT_DICTS, _CARDINALITY_SLOT_SORT_DICTS = [], []
_WIDTH_CENTER_SLOT_CAR_DICTS, _WIDTH_SKEW_SLOT_DICT = [], {}
_SORT_LEVELS = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.01]
_CAR_LEVELS = [0, 1e-8, 1e-7, 1e-6, 1e-5, 1e-4, 1e-3, 1e-2, 1e-1, 1.01]


def init_one_slot(col_num, interval_list, prob):
    res_dict = {}
    is_str = isinstance(interval_list[0], str)
    # str list is skew_pattern, where prob_slot_num is equal to len(pattern_list)
    # other list is interval_list, where prob_slot_num is equal to len(interval_list) - 1
    if not is_str:
        if len(interval_list) != len(prob) + 1:
            LOG.error("interval_list and prob not match", stack_info=True)
            exit(1)
        for i in range(len(prob)):
            key = (interval_list[i], interval_list[i + 1])
            if isnan(prob[i]):
                slot_val = 0
                if col_num > 0:
                    LOG.error(f"prob is nan, but col_num: {col_num}, interval_list: {interval_list}", stack_info=True)
                    exit(1)
            else:
                slot_val = int(prob[i] * col_num + 0.5)
            res_dict[key] = slot_val
        slot_val_sum = sum(res_dict.values())
        # fill the slot_val_sum to col_num by adding the rest to the most possible slot
        if slot_val_sum < col_num:
            max_prob_idx = np.argmax(prob)
            res_dict[(interval_list[max_prob_idx], interval_list[max_prob_idx + 1])] += col_num - slot_val_sum
        # if slot_val_sum is larger, then each slot reduce by 1 while keeping non-negative
        if slot_val_sum > col_num:
            for key in res_dict.keys():
                if res_dict[key] > 0:
                    res_dict[key] -= 1
                    if sum(res_dict.values()) == col_num:
                        break
        if sum(res_dict.values()) != col_num:
            LOG.error(f"slot_val_sum:{sum(res_dict.values())},col_num:{col_num}", stack_info=True)
            exit(1)
    else:
        if len(interval_list) != len(prob):
            LOG.error("interval_list and prob not match")
            return -1
        for i in range(len(prob)):
            key = interval_list[i]
            if isnan(prob[i]):
                slot_val = 0
                if col_num > 0:
                    LOG.error(f"str_prob is nan, but col_num: {col_num}, interval_list: {interval_list}")
                    exit(1)
            else:
                slot_val = int(prob[i] * col_num + 0.5)
            res_dict[key] = slot_val
        slot_val_sum = sum(res_dict.values())
        # fill the slot_val_sum to col_num by adding the rest to the most possible slot
        if slot_val_sum < col_num:
            max_prob_idx = np.argmax(prob)
            res_dict[interval_list[max_prob_idx]] += col_num - slot_val_sum
        # if slot_val_sum is larger, then each slot reduce by 1 while keeping non-negative
        if slot_val_sum > col_num:
            for key in res_dict.keys():
                if res_dict[key] > 0:
                    res_dict[key] -= 1
                    if sum(res_dict.values()) == col_num:
                        break
        if sum(res_dict.values()) != col_num:
            LOG.error(f"slot_val_sum:{sum(res_dict.values())},col_num:{col_num}", stack_info=True)
            exit(1)

    return res_dict


def init_all_slot(col_num, js_reader):
    global _NULL_RATIO_SLOT_DICT, _SORT_SCORE_SLOT_DICT, _SORTED_WINDOW_SIZE, _CONTINUE_MEAN_CUT
    global _CONTINUE_MEAN_SLOT_SORT_DICTS, _REPEAT_NUM_CUT, _CONTINUE_MEAN_MAP_REPEAT_BLOCK_RATIO
    global _SKEW_PATTERN_SLOT_SORT_DICTS, _BI_SKEW_SLOT_SORT_DICTS, _GENTLE_ZIPF_SKEW_SLOT_SORT_DICTS
    global _HOTSPOT_FIRST_SLOT_SORT_DICTS, _HOTSPOT_SKEW_1_SLOT_SORT_DICTS, _HOTSPOT_SKEW_2_SLOT_SORT_DICTS
    global _IS_CATEGORICAL_SLOT_SORT_DICTS
    global _UNIQUE_NUM_SLOT_SORT_DICTS, _CARDINALITY_SLOT_SORT_DICTS
    global _WIDTH_CENTER_SLOT_CAR_DICTS, _WIDTH_SKEW_SLOT_DICT

    # null config
    null_fea = js_reader['null_fea']
    _NULL_RATIO_SLOT_DICT = init_one_slot(col_num, null_fea['null_ratio'], null_fea['null_ratio_prob'])

    # sort config
    sort_fea = js_reader['sort_fea']
    _SORTED_WINDOW_SIZE = int(sort_fea['sort_window'])
    _SORT_SCORE_SLOT_DICT = init_one_slot(col_num, sort_fea['sort_score'], sort_fea['sort_score_prob'])
    # sort_low and sort_high are separated
    sort_col_num_level_list = [0] * (len(_SORT_LEVELS) - 1)
    for key in _SORT_SCORE_SLOT_DICT.keys():
        for i in range(len(_SORT_LEVELS) - 1):
            if _SORT_LEVELS[i] <= key[0] and key[0] < _SORT_LEVELS[i + 1]:
                sort_col_num_level_list[i] += _SORT_SCORE_SLOT_DICT[key]
                break

    # continuous repeat config
    repeat_fea = js_reader['continuous_repeat_fea']
    _CONTINUE_MEAN_CUT = repeat_fea['continuous_mean_cut']
    _CONTINUE_MEAN_SLOT_SORT_DICTS = [{}] * len(sort_col_num_level_list)
    for i in range(len(sort_col_num_level_list)):
        _CONTINUE_MEAN_SLOT_SORT_DICTS[i] = init_one_slot(sort_col_num_level_list[i], repeat_fea['continuous_mean_cut'],
                                                          repeat_fea[f'continuous_mean_sort{i}_prob'])
    _REPEAT_NUM_CUT = repeat_fea['repeat_num_cut']
    _CONTINUE_MEAN_MAP_REPEAT_BLOCK_RATIO = repeat_fea['continue_mean_map_repeat_block_ratio']

    # skew config, sort_low and sort_high are separated
    skew_fea = js_reader['skew_fea']
    _SKEW_PATTERN_SLOT_SORT_DICTS = [{}] * len(sort_col_num_level_list)
    _BI_SKEW_SLOT_SORT_DICTS = [{}] * len(sort_col_num_level_list)
    _GENTLE_ZIPF_SKEW_SLOT_SORT_DICTS = [{}] * len(sort_col_num_level_list)
    _HOTSPOT_FIRST_SLOT_SORT_DICTS = [{}] * len(sort_col_num_level_list)
    _HOTSPOT_SKEW_1_SLOT_SORT_DICTS = [{}] * len(sort_col_num_level_list)
    _HOTSPOT_SKEW_2_SLOT_SORT_DICTS = [{}] * len(sort_col_num_level_list)
    for i in range(len(sort_col_num_level_list)):
        _SKEW_PATTERN_SLOT_SORT_DICTS[i] = init_one_slot(sort_col_num_level_list[i], skew_fea['skew_pattern'],
                                                         skew_fea[f'skew_pattern_sort{i}_prob'])
        _BI_SKEW_SLOT_SORT_DICTS[i] = init_one_slot(_SKEW_PATTERN_SLOT_SORT_DICTS[i]['binary'], skew_fea['bi_skew'],
                                                    skew_fea[f'bi_skew_sort{i}_prob'])
        _GENTLE_ZIPF_SKEW_SLOT_SORT_DICTS[i] = init_one_slot(_SKEW_PATTERN_SLOT_SORT_DICTS[i]['gentle_zipf'], skew_fea['gentle_zipf_skew'],
                                                             skew_fea[f'gentle_zipf_skew_sort{i}_prob'])
        hotspot_col_num = _SKEW_PATTERN_SLOT_SORT_DICTS[i]['hotspot'] + _SKEW_PATTERN_SLOT_SORT_DICTS[i]['gentle_zipf']
        _HOTSPOT_FIRST_SLOT_SORT_DICTS[i] = init_one_slot(hotspot_col_num, skew_fea['hotspot_first'],
                                                          skew_fea[f'hotspot_first_sort{i}_prob'])
        _HOTSPOT_SKEW_1_SLOT_SORT_DICTS[i] = init_one_slot(hotspot_col_num, skew_fea['hotspot_skew'],
                                                           skew_fea[f'hotspot_skew_1_sort{i}_prob'])
        _HOTSPOT_SKEW_2_SLOT_SORT_DICTS[i] = init_one_slot(hotspot_col_num, skew_fea['hotspot_skew'],
                                                           skew_fea[f'hotspot_skew_2_sort{i}_prob'])

    # cnt config, sort_low and sort_high are separated
    cnt_fea = js_reader['cnt_fea']
    _IS_CATEGORICAL_SLOT_SORT_DICTS = [{}] * len(sort_col_num_level_list)
    _UNIQUE_NUM_SLOT_SORT_DICTS = [{}] * len(sort_col_num_level_list)
    _CARDINALITY_SLOT_SORT_DICTS = [{}] * len(sort_col_num_level_list)
    for i in range(len(sort_col_num_level_list)):
        # cnt is calculated from unique_num=3, so reduce some pattern
        cnt_col_num = sort_col_num_level_list[i] - _SKEW_PATTERN_SLOT_SORT_DICTS[i]['null'] - _SKEW_PATTERN_SLOT_SORT_DICTS[i][
            'single'] - _SKEW_PATTERN_SLOT_SORT_DICTS[i]['binary']
        # unique_num is used for categorical data, and cardinality_ratio is used for others
        unique_num_col_num = int(cnt_col_num * cnt_fea[f'is_categorical_sort{i}'] + 0.5)
        cardinality_col_num = cnt_col_num - unique_num_col_num
        _IS_CATEGORICAL_SLOT_SORT_DICTS[i] = {'is': unique_num_col_num, 'not': cardinality_col_num}
        _UNIQUE_NUM_SLOT_SORT_DICTS[i] = init_one_slot(unique_num_col_num, cnt_fea['unique_num'], cnt_fea[f'unique_num_sort{i}_prob'])
        _CARDINALITY_SLOT_SORT_DICTS[i] = init_one_slot(cardinality_col_num, cnt_fea['cardinality'], cnt_fea[f'cardinality_sort{i}_prob'])

    # width config, depend on cardinality_ratio
    width_fea = js_reader['width_fea']
    # tmp_car_col_num_list[0]=10, means 10 columns with cardinality of [0,1e-8)
    tmp_car_col_num_list = [0] * (len(_CAR_LEVELS) - 1)
    for i in range(len(sort_col_num_level_list)):
        car_col_num_dict = _CARDINALITY_SLOT_SORT_DICTS[i]
        unique_col_num_dict = _UNIQUE_NUM_SLOT_SORT_DICTS[i]
        for j, key in enumerate(car_col_num_dict.keys()):
            tmp_car_col_num_list[j] += car_col_num_dict[key]
        # unique_num just have [2,10,100,1000], corresponding to 2nd lowest card [1e-8,1e-7,1e-6,1e-5)
        for j, key in enumerate(unique_col_num_dict.keys()):
            tmp_car_col_num_list[j + 1] += unique_col_num_dict[key]
        tmp_car_col_num_list[0] += _SKEW_PATTERN_SLOT_SORT_DICTS[i]['null']
        tmp_car_col_num_list[1] += _SKEW_PATTERN_SLOT_SORT_DICTS[i]['single'] + _SKEW_PATTERN_SLOT_SORT_DICTS[i]['binary']
    if sum(tmp_car_col_num_list) != col_num:
        LOG.error(f'width center config error, sum of tmp_car_col_num_list={tmp_car_col_num_list} is not equal to col_num={col_num}')
        exit(1)
    _WIDTH_CENTER_SLOT_CAR_DICTS = [{}] * len(tmp_car_col_num_list)
    for i in range(len(tmp_car_col_num_list)):
        _WIDTH_CENTER_SLOT_CAR_DICTS[i] = init_one_slot(tmp_car_col_num_list[i], width_fea['width_center'],
                                                        width_fea[f'width_center_car{i}_prob'])
    # assume width skew is independent with width center and unique_num
    _WIDTH_SKEW_SLOT_DICT = init_one_slot(col_num, width_fea['width_skew'], width_fea['width_skew_prob'])


def get_config_per_col(global_dict):
    key_list = list(global_dict.keys())
    random.shuffle(key_list)
    find_slot = False
    for key in key_list:
        if global_dict[key] > 0:
            global_dict[key] -= 1
            find_slot = True
            break
    if not find_slot:
        LOG.error(f"global_dict no enough slot_val")
        print("global dict:", global_dict)
        exit(1)
    is_tuple = isinstance(key, tuple)
    if not is_tuple:
        res = key
    else:
        res = random.uniform(key[0], key[1])
        res = round(res, 10)
        if res == key[1]:
            res = key[1] - 1e-4
    return res


def rewrite_config(DATA_TYPE, ROW_NUM, output_file):

    # null config
    NULL_RATIO = get_config_per_col(_NULL_RATIO_SLOT_DICT)
    NULL_RATIO = 0 if NULL_RATIO <= 1e-8 else NULL_RATIO

    # sort config
    SORTED_WINDOW_SIZE = _SORTED_WINDOW_SIZE
    SORT_SCORE = get_config_per_col(_SORT_SCORE_SLOT_DICT)
    for idx, val in enumerate(_SORT_LEVELS):
        if SORT_SCORE < val:
            break
    sort_level_idx = idx - 1

    # continue config
    continue_mean = get_config_per_col(_CONTINUE_MEAN_SLOT_SORT_DICTS[sort_level_idx])
    REPEAT_NUM_CUT = _REPEAT_NUM_CUT
    for idx, val in enumerate(_CONTINUE_MEAN_CUT):
        if continue_mean < _CONTINUE_MEAN_CUT[idx]:
            break
    REPEAT_BLOCK_RATIO = _CONTINUE_MEAN_MAP_REPEAT_BLOCK_RATIO[idx - 1]

    # skew config
    BI_SKEW, HOTSPOT_FIRST, HOTSPOT_SECOND, HOTSPOT_THIRD, ZIPF_SKEW = 0, 0, 0, 0, 0
    SKEW_PARTTERN = get_config_per_col(_SKEW_PATTERN_SLOT_SORT_DICTS[sort_level_idx])
    if SKEW_PARTTERN == 'binary':
        BI_SKEW = get_config_per_col(_BI_SKEW_SLOT_SORT_DICTS[sort_level_idx])
    elif SKEW_PARTTERN == 'gentle_zipf':
        ZIPF_SKEW = get_config_per_col(_GENTLE_ZIPF_SKEW_SLOT_SORT_DICTS[sort_level_idx])
    if SKEW_PARTTERN == 'hotspot' or SKEW_PARTTERN == 'gentle_zipf':
        HOTSPOT_FIRST = get_config_per_col(_HOTSPOT_FIRST_SLOT_SORT_DICTS[sort_level_idx])
        hotspot_skew_1 = get_config_per_col(_HOTSPOT_SKEW_1_SLOT_SORT_DICTS[sort_level_idx])
        hotspot_skew_2 = get_config_per_col(_HOTSPOT_SKEW_2_SLOT_SORT_DICTS[sort_level_idx])
        HOTSPOT_SECOND = HOTSPOT_FIRST / hotspot_skew_1
        HOTSPOT_THIRD = HOTSPOT_FIRST / hotspot_skew_2
        sum = HOTSPOT_FIRST + HOTSPOT_SECOND + HOTSPOT_THIRD
        if sum > 1:
            HOTSPOT_FIRST = HOTSPOT_FIRST / sum
            HOTSPOT_SECOND = HOTSPOT_SECOND / sum
            HOTSPOT_THIRD = HOTSPOT_THIRD / sum

    # cnt config
    cnt_from_skew_pattern = True
    is_cate = 'undefined'
    if SKEW_PARTTERN == 'null':
        UNIQUE_NUM = 0
        CARDINALITY = 0
    elif SKEW_PARTTERN == 'single':
        UNIQUE_NUM = 1
        CARDINALITY = UNIQUE_NUM / ROW_NUM
    elif SKEW_PARTTERN == 'binary':
        UNIQUE_NUM = 2
        CARDINALITY = UNIQUE_NUM / ROW_NUM
    else:
        cnt_from_skew_pattern = False
        is_cate = get_config_per_col(_IS_CATEGORICAL_SLOT_SORT_DICTS[sort_level_idx])
        # if categorical, directly use unique_num else use cardinality
        # and the consistent config_file should contain both unique_num and cardinality
        if is_cate == 'is':
            UNIQUE_NUM = int(get_config_per_col(_UNIQUE_NUM_SLOT_SORT_DICTS[sort_level_idx]) + 1)
            CARDINALITY = UNIQUE_NUM / ROW_NUM
        else:
            CARDINALITY = get_config_per_col(_CARDINALITY_SLOT_SORT_DICTS[sort_level_idx])
            UNIQUE_NUM = int(CARDINALITY * ROW_NUM + 3)

    # width config
    if is_cate != 'not':
        tmp_car = UNIQUE_NUM / 1e8  # try to fix the bug from different row_num
    else:
        tmp_car = CARDINALITY
    for idx, val in enumerate(_CAR_LEVELS):
        if tmp_car < val:
            break
    car_level_idx = idx - 1
    # genrate a list like [car_level_idx, car_level_idx + 1, car_level_idx - 1,...]
    tolarant_list = [car_level_idx]
    for i in range(1, 10):
        tolarant_list = tolarant_list + [car_level_idx + i, car_level_idx - i]
    is_slot_enough = False
    for tmp_idx in tolarant_list:
        if tmp_idx < 0 or tmp_idx >= len(_WIDTH_CENTER_SLOT_CAR_DICTS):
            continue
        sum_slot = 0
        for key in _WIDTH_CENTER_SLOT_CAR_DICTS[tmp_idx]:
            sum_slot += _WIDTH_CENTER_SLOT_CAR_DICTS[tmp_idx][key]
        if sum_slot > 0:
            is_slot_enough = True
            break
    WIDTH_CENTER = get_config_per_col(_WIDTH_CENTER_SLOT_CAR_DICTS[tmp_idx])
    # else:
    #     # get the key of largest value in _WIDTH_CENTER_UL_SLOT_DICT
    #     largest_tuple = max(_WIDTH_CENTER_UL_SLOT_DICT, key=_WIDTH_CENTER_UL_SLOT_DICT.get)
    #     WIDTH_CENTER = largest_tuple[0]
    WIDTH_SKEW = get_config_per_col(_WIDTH_SKEW_SLOT_DICT)

    # long string has quite special features so we should re-correct the configs
    if WIDTH_CENTER >= 100 and DATA_TYPE == 'string':
        CARDINALITY = 0.8 if CARDINALITY < 0.8 else CARDINALITY
        UNIQUE_NUM = int(CARDINALITY * ROW_NUM + 3)
        NULL_RATIO = 0
        SORT_SCORE = 0.25
        SKEW_PARTTERN = 'gentle_zipf'
        HOTSPOT_FIRST, HOTSPOT_SECOND, HOTSPOT_THIRD = 0.07, 0.02, 0.01
        ZIPF_SKEW = 0.5

    basic_config = {"data_type": DATA_TYPE, "row_num": ROW_NUM}
    cnt_fea = {"unique_num": UNIQUE_NUM, "cardinality": CARDINALITY}
    null_fea = {"null_ratio": NULL_RATIO}
    sort_fea = {"sort_window": SORTED_WINDOW_SIZE, "sort_score": SORT_SCORE}
    repeat_fea = {"repeat_num_cut": REPEAT_NUM_CUT, "repeat_num_ratio_list": REPEAT_BLOCK_RATIO}
    width_fea = {"width_center": WIDTH_CENTER, "width_skew": WIDTH_SKEW}
    skew_fea = {
        "skew_pattern": SKEW_PARTTERN,
        "bi_skew": BI_SKEW,
        "hotspot_first": HOTSPOT_FIRST,
        "hotspot_second": HOTSPOT_SECOND,
        "hotspot_third": HOTSPOT_THIRD,
        "zipf_skew": ZIPF_SKEW
    }
    all_config = {
        "basic_config": basic_config,
        "cnt_fea": cnt_fea,
        "null_fea": null_fea,
        "sort_fea": sort_fea,
        "repeat_fea": repeat_fea,
        "width_fea": width_fea,
        "skew_fea": skew_fea
    }

    # write to json output_config_file
    with open(output_file, 'w') as f:
        json.dump(all_config, f, indent=4)

    return all_config


def gen_schema(tar_dir, fea_dict, TARGET_BASEFILE_NAME):
    f = open(os.path.join(tar_dir, f'{TARGET_BASEFILE_NAME}_arrow_schema.txt'), "w")
    f.write("message schema {\n")
    for i in range(COL_NUM):
        if fea_dict[f'col{i}']['basic_config']['data_type'] == 'int':
            f.write(f"  optional int64 col{i};\n")
        elif fea_dict[f'col{i}']['basic_config']['data_type'] == 'float':
            f.write(f"  optional double col{i};\n")
        elif fea_dict[f'col{i}']['basic_config']['data_type'] == 'string':
            f.write(f"  optional binary col{i} (String);\n")
        else:
            assert False
    f.write("}")
    f.close()
    f = open(os.path.join(tar_dir, f'{TARGET_BASEFILE_NAME}_orc_schema.txt'), "w")
    f.write("struct<")
    for i in range(COL_NUM):
        if fea_dict[f'col{i}']['basic_config']['data_type'] == 'int':
            f.write(f"f{i}:bigint")
        elif fea_dict[f'col{i}']['basic_config']['data_type'] == 'float':
            f.write(f"f{i}:double")
        elif fea_dict[f'col{i}']['basic_config']['data_type'] == 'string':
            f.write(f"f{i}:string")
        else:
            assert False
        if i != COL_NUM - 1:
            f.write(",")
    f.write(">")
    f.close()

# CAUTION: fea_type only works after assign single_type
def gen_core_data(ROW_NUM, COL_NUM, TARGET_BASEFILE_NAME, single_type=None, fea_type=None, fea_param=None):
    core_config_dict = {
        "int": json.load(open("./workload_config/core_config/core_int64.json", 'r')),
        "float": json.load(open("./workload_config/core_config/core_float64.json", 'r')),
        "string": json.load(open("./workload_config/core_config/core_object.json", 'r'))
    }
    if single_type is None:
        dtype_dict = {'int': 0.3773, 'float': 0.2106, 'string': 0.4121}  # consider both bi and others
        dtype_normalized_prob = [0.3773, 0.2106, 0.4121]
    else:
        dtype_dict = {
            'int': int(single_type == 'int'),
            'float': int(single_type == 'float'),
            'string': int(single_type == 'string')
        }  # consider both bi and others
        dtype_normalized_prob = [int(single_type == 'int'), int(single_type == 'float'), int(single_type == 'string')]
        if fea_type == 'sort':
            core_config_dict[single_type]['sort_fea']['sort_score_prob'] = [1]
            core_config_dict[single_type]['sort_fea']['sort_score'] = [fea_param * 0.99, fea_param]
        elif fea_type == 'car':
            core_config_dict[single_type]['null_ratio'] = [0, 0]
            core_config_dict[single_type]['null_ratio_prob'] = [1]
            skew_keys = ['skew_fea']
            for key in skew_keys:
                # core_config_dict[single_type][key]['skew_pattern'] = [
                #             "uniform",
                #             "hotspot",
                #             "gentle_zipf"
                #         ]
                # make sure the last three pattern's prob add up to 1
                for i in range(10):
                    key2 = f'skew_pattern_sort{i}_prob'
                    prob_sum = sum(core_config_dict[single_type][key][key2][3:])
                    core_config_dict[single_type][key][key2] = list(map(lambda x:x/prob_sum, core_config_dict[single_type][key][key2]))
                    core_config_dict[single_type][key][key2][0] = 0
                    core_config_dict[single_type][key][key2][1] = 0
                    core_config_dict[single_type][key][key2][2] = 0
            origin_car_cut = core_config_dict[single_type]['cnt_fea']['cardinality']
            for car_idx in range(1, len(origin_car_cut)):
                if fea_param <= origin_car_cut[car_idx]:
                    core_config_dict[single_type]['cnt_fea']['cardinality'][car_idx] = fea_param
                    core_config_dict[single_type]['cnt_fea']['cardinality'][car_idx - 1] = fea_param * 0.99
                    for i in range(10):
                        core_config_dict[single_type]['cnt_fea'][f'cardinality_sort{i}_prob'] = [0] * (len(origin_car_cut) - 1)
                        core_config_dict[single_type]['cnt_fea'][f'cardinality_sort{i}_prob'][car_idx - 1] = 1
                        core_config_dict[single_type]['cnt_fea'][f'is_categorical_sort{i}'] = 0
                    break
        elif fea_type == 'zipf':
            key = 'skew_fea'
            # core_config_dict[single_type][key]['skew_pattern'] = [
            #             "gentle_zipf"
            #         ]
            for i in range(10):
                core_config_dict[single_type][key][f'skew_pattern_sort{i}_prob'] = [0, 0, 0, 0, 0, 1]
                core_config_dict[single_type][key][f'gentle_zipf_skew_sort{i}_prob'] = [1]
            core_config_dict[single_type][key]['gentle_zipf_skew'] = [fea_param * 0.99, fea_param]
        elif fea_type == 'width':
            key = 'width_fea'
            core_config_dict[single_type][key]['width_center'] = [fea_param * 0.99, fea_param]
            for i in range(9):
                core_config_dict[single_type][key][f'width_center_car{i}_prob'] = [1]
        else:
            assert False
    data_type_slot_dict = {'int': 0, 'float': 0, 'string': 0}
    for col_idx in range(COL_NUM):
        for i in range(len(dtype_normalized_prob)):
            if col_idx < COL_NUM * sum(dtype_normalized_prob[:i + 1]):
                tmp_data_type = list(dtype_dict.keys())[i]
                data_type_slot_dict[tmp_data_type] += 1
                break
    # generate the data
    df_list = []
    os.system(f"rm -rf ./{TARGET_BASEFILE_NAME}/configs/core")
    os.system(f"mkdir -p ./{TARGET_BASEFILE_NAME}/configs/core")
    global_idx = 0
    table_config = {}
    seed_list = []
    random.seed(53)  # hardcode a list
    for i in range(COL_NUM):
        seed_list.append(random.randint(0, 1000000))
    random.seed(53)  # hardcode a list
    for DATA_TYPE in data_type_slot_dict.keys():
        col_num_per_dtype = data_type_slot_dict[DATA_TYPE]
        LOG.info("Start core_workload, data_type: {}, col_num: {}".format(DATA_TYPE, col_num_per_dtype))
        init_all_slot(col_num_per_dtype, js_reader=core_config_dict[DATA_TYPE])
        for col_idx in range(col_num_per_dtype):
            output_config_file = f'./{TARGET_BASEFILE_NAME}/configs/core/{DATA_TYPE}_{col_idx}.json'
            col_config = rewrite_config(DATA_TYPE, ROW_NUM, output_config_file)
            table_config[f'col{global_idx}'] = col_config
            global_idx += 1
            # import generator to get data
            generator = Generator(output_config_file, seed=seed_list[col_idx])
            res_data_list = generator.gen_data()
            # write to file
            # Attention!! If there is any null data in int column, pandas will convert the column to float
            res_df = pd.DataFrame(res_data_list)
            if DATA_TYPE == 'string':
                res_df = res_df.replace({'null': np.nan})
            # FIXME: randomly generate all null values in float (UNIQUE_NUM=0)
            if DATA_TYPE == 'int':
                res_df = res_df.astype('Int64')
            if res_df.isnull().values.all():
                if DATA_TYPE == 'string':
                    res_df.iloc[0] = 'a'
                elif DATA_TYPE == 'int':
                    res_df.iloc[0] = 1
                else:
                    res_df.iloc[0] = 1.0
            df_list.append(res_df)
            LOG.info(f"Finish col_idx: {col_idx}, DATA_TYPE: {DATA_TYPE}")
    with open(f'./{TARGET_BASEFILE_NAME}/configs/table_config.json', 'w') as f:
        json.dump(table_config, f, indent=4)
    res_df = pd.concat(df_list, axis=1)
    tar_dir = f"./{TARGET_BASEFILE_NAME}/gen_data"
    os.makedirs(tar_dir, exist_ok=True)
    gen_schema(tar_dir, table_config, TARGET_BASEFILE_NAME)
    res_df.to_csv(os.path.join(tar_dir, f'{TARGET_BASEFILE_NAME}.csv'), header=False, index=False)
    print("Finish core dataset! 🎉🎉🎉🎉🎉🎉")


def gen_multi_data(ROW_NUM, COL_NUM, TARGET_BASEFILE_NAME, WORKLOAD):
    if WORKLOAD not in ['geo', 'classic', 'log', 'ml', 'bi', 'multiple']:
        LOG.error("wrong workload tag:", WORKLOAD)
        exit(1)
    if WORKLOAD == 'multiple':
        workload_lists = ['geo', 'classic', 'log', 'ml', 'bi']
    else:
        workload_lists = [WORKLOAD]
    # workload_lists = ['classic']
    data_type_proportion = {
        'classic': {
            'int': 45,
            'float': 8,
            'string': 85
        },
        'geo': {
            'int': 15,
            'float': 4,
            'string': 30
        },
        'log': {
            'int': 12,
            'float': 19,
            'string': 18
        },
        'ml': {
            'int': 22,
            'float': 45,
            'string': 32 
        },
        'bi': {
            'int': 1150,
            'float': 494,
            'string': 858
        }
    }

    SANITY_CHECK = False
    if SANITY_CHECK:
        print("ATTENTION!! The generator is in sanity check mode!")

    for workload in workload_lists:
        workload_config_dict = {
            "int": json.load(open("./workload_config/{}_config/{}_int64.json".format(workload, workload), 'r')),
            "float": json.load(open("./workload_config/{}_config/{}_float64.json".format(workload, workload), 'r')),
            "string": json.load(open("./workload_config/{}_config/{}_object.json".format(workload, workload), 'r'))
        }

        data_type_slot_dict = {'int': 0, 'float': 0, 'string': 0}
        # get the data type status of the current workload
        if SANITY_CHECK:
            # directly read the datatypes from the outside json file
            with open('./sanity_check/workload_check/real_data_type.json', 'r') as f:
                real_data_type = json.load(f)
            dtype_dict = real_data_type[workload]
            COL_NUM = sum(dtype_dict.values())
            data_type_slot_dict = dtype_dict  # just use the same dtype_dict
        else:
            dtype_dict = data_type_proportion[workload]
            dtype_normalized_prob = [dtype_dict[key] / sum(dtype_dict.values()) for key in dtype_dict]
            # assign the data type to columns according to the propotion
            for col_idx in range(COL_NUM):
                for i in range(len(dtype_normalized_prob)):
                    if col_idx < COL_NUM * sum(dtype_normalized_prob[:i + 1]):
                        tmp_data_type = list(dtype_dict.keys())[i]
                        data_type_slot_dict[tmp_data_type] += 1
                        break

        # generate the data
        df_list = []
        os.system(f"rm -rf ./{TARGET_BASEFILE_NAME}/configs/{workload}")
        os.system(f"mkdir -p ./{TARGET_BASEFILE_NAME}/configs/{workload}")
        global_idx = 0
        table_config = {}
        seed_list = []
        random.seed(53)  # hardcode a list
        for i in range(COL_NUM):
            seed_list.append(random.randint(0, 1000000))
        random.seed(53)  # hardcode a list
        for DATA_TYPE in data_type_slot_dict.keys():
            col_num_per_dtype = data_type_slot_dict[DATA_TYPE]
            LOG.info("Start workload: {}, data_type: {}, col_num: {}".format(workload, DATA_TYPE, col_num_per_dtype))
            init_all_slot(col_num_per_dtype, js_reader=workload_config_dict[DATA_TYPE])
            for col_idx in range(col_num_per_dtype):
                output_config_file = f'./{TARGET_BASEFILE_NAME}/configs/{workload}/{DATA_TYPE}_{col_idx}.json'
                col_config = rewrite_config(DATA_TYPE, ROW_NUM, output_config_file)
                table_config[f'col{global_idx}'] = col_config
                global_idx += 1
                # import generator to get data
                generator = Generator(output_config_file, seed=seed_list[col_idx])
                res_data_list = generator.gen_data()
                # write to file
                # Attention!! If there is any null data in int column, pandas will convert the column to float
                res_df = pd.DataFrame(res_data_list)
                if DATA_TYPE == 'string':
                    res_df = res_df.replace({'null': np.nan})
                if DATA_TYPE == 'int':
                    res_df = res_df.astype('Int64')
                if res_df.isnull().values.all():
                    if DATA_TYPE == 'string':
                        res_df.iloc[0] = 'a'
                    elif DATA_TYPE == 'int':
                        res_df.iloc[0] = 1
                    else:
                        res_df.iloc[0] = 1.0
                df_list.append(res_df)
                LOG.info(f"Finish col_idx: {col_idx}, DATA_TYPE: {DATA_TYPE}")
        # write the table config to file
        with open(f'./{TARGET_BASEFILE_NAME}/configs/table_config.json', 'w') as f:
            json.dump(table_config, f, indent=4)
        res_df = pd.concat(df_list, axis=1)
        tar_dir = f"./{TARGET_BASEFILE_NAME}/gen_data"
        os.makedirs(tar_dir, exist_ok=True)
        gen_schema(tar_dir, table_config, TARGET_BASEFILE_NAME)
        res_df.to_csv(os.path.join(tar_dir, f'{TARGET_BASEFILE_NAME}.csv'), header=False, index=False)
        print("Finish workload: {}! 🎉🎉🎉🎉🎉🎉".format(workload))


if __name__ == '__main__':
    # receive args from command line
    if len(sys.argv) != 5 and len(sys.argv) != 7:
        print("Usage: python3 gen_workloads.py <workload> <row_num> <col_num> <target_basefile_name>")
        exit(1)
    WORKLOAD = str(sys.argv[1])  # core or multiple or classic, geo, log, ml
    ROW_NUM = int(sys.argv[2])
    COL_NUM = int(sys.argv[3])
    # No need to set data_type here, which just follows the proportion of each workload
    TARGET_BASEFILE_NAME = str(sys.argv[4])
    if len(sys.argv) >= 7:
        fea_type = str(sys.argv[5])
        fea_param = float(sys.argv[6])
    else:
        fea_type = None
        fea_param = None
    # change the dir of the current file
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    if WORKLOAD == "core":
        gen_core_data(ROW_NUM, COL_NUM, TARGET_BASEFILE_NAME)
    elif WORKLOAD in ['int', 'float', 'string']:
        gen_core_data(ROW_NUM, COL_NUM, TARGET_BASEFILE_NAME, WORKLOAD, fea_type, fea_param)
    else:
        gen_multi_data(ROW_NUM, COL_NUM, TARGET_BASEFILE_NAME, WORKLOAD)
    print("Generating Done!!")