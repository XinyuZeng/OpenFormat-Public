from cmath import isnan, nan
from math import sqrt
import string
from random import Random
import time
import pandas as pd
import numpy as np
import json
import os
import sys


class Generator(object):
    def __init__(self, config_file, seed=None) -> None:
        """ read the config file and set the parameters """
        if seed != None:
            self.myrandom = Random(seed)
            self.nprandom = np.random.RandomState(seed=seed)
        else:
            self.myrandom = Random()
            self.nprandom = np.random.RandomState()

        with open(config_file, "r") as f:
            js_reader = json.load(f)

        # basic config
        basic_config = js_reader["basic_config"]
        self.__row_num = basic_config["row_num"]
        self.__data_type = basic_config["data_type"]
        # self.__target_file_name = basic_config["target_file_name"]
        # self.__col_num = basic_config["col_num"]

        # null fea
        null_fea = js_reader["null_fea"]
        self.__null_ratio = null_fea["null_ratio"]

        # sort fea
        sort_fea = js_reader["sort_fea"]
        self.__sorted_window_size = int(sort_fea["sort_window"])
        self.__sort_score = sort_fea["sort_score"]

        # repeat fea
        repeat_fea = js_reader["repeat_fea"]
        self.__repeat_num_cut = repeat_fea["repeat_num_cut"]
        repeat_num_ratio_list = repeat_fea['repeat_num_ratio_list']
        actual_space_ratio_list = []
        for idx in range(len(self.__repeat_num_cut) - 1):
            adopted_num = (self.__repeat_num_cut[idx + 1] + self.__repeat_num_cut[idx] + 1) // 2  # (0,1] -> 1, (1,2] -> 2
            actual_space_ratio_list.append(repeat_num_ratio_list[idx] * adopted_num)
        factor = self.__row_num / sum(actual_space_ratio_list)  # one unit space should contain how many rows
        # how many blocks for each adopted_num = adopted actual_space_ratio * factor / adopted_num = repeat_num_ratio * factor
        self.__repeat_num_list = [int(x * factor) for x in repeat_num_ratio_list]

        # skew fea
        skew_fea = js_reader["skew_fea"]
        self.__skew_pattern = skew_fea["skew_pattern"]
        self.__bi_skew = skew_fea["bi_skew"]
        self.__hotspot_first = skew_fea["hotspot_first"]
        self.__hotspot_second = skew_fea["hotspot_second"]
        self.__hotspot_third = skew_fea["hotspot_third"]
        self.__gentle_zipf_skew = skew_fea["zipf_skew"]

        # cnt fea
        cnt_fea = js_reader["cnt_fea"]
        self.__unique_num = int(cnt_fea["unique_num"])
        # when unique_num >50000, is not catogorical, so the cardinality would grow with the row_num
        if self.__unique_num > self.__row_num or self.__unique_num > 50000:
            self.__unique_num = int(1 + self.__row_num * cnt_fea["cardinality"])

        # width config
        width_fea = js_reader['width_fea']
        self.__width_center = width_fea['width_center']
        self.__width_skew = width_fea['width_skew']

    def __preprocess_width(self, step_len):
        lh1 = int(self.__width_center / sqrt(step_len))
        rh1 = int(self.__width_center * sqrt(step_len))
        lh2 = int(lh1 / step_len)
        rh2 = int(rh1 * step_len)
        lh3 = int(lh2 / step_len)
        rh3 = int(rh2 * step_len)
        interval_list = [lh3, lh2, lh1, rh1, rh2, rh3]
        for i in range(0, len(interval_list) - 1):  # avoid the lists like [0,0,0,4,8,16]
            if interval_list[i] >= interval_list[i + 1]:
                interval_list[i + 1] = interval_list[i] + 1
        prob_list = [0, 0, 0, 0, 0]
        if self.__width_skew >= 0.5:
            prob_list[1] = (1 - self.__width_skew) / 2
            prob_list[2] = self.__width_skew
            prob_list[3] = (1 - self.__width_skew) / 2
        else:
            prob_list[0] = prob_list[4] = (1 - self.__width_skew) / 6
            prob_list[1] = prob_list[3] = (1 - self.__width_skew) / 3
            prob_list[2] = self.__width_skew
        # multiply the null_ratio to the prob_list elements
        width_prob_list = [x * (1 - self.__null_ratio) for x in prob_list]
        return interval_list, width_prob_list

    def __get_skewed_list(self):
        # "single", "binary", "uniform", "hotspot", "gentle_zipf"
        valid_ratio = 1 - self.__null_ratio
        if self.__unique_num == 0:
            return []
        elif self.__skew_pattern == "single":
            return [1 * valid_ratio]
        elif self.__skew_pattern == "binary":
            return [self.__bi_skew * valid_ratio, (1 - self.__bi_skew) * valid_ratio]
        elif self.__skew_pattern == "uniform":
            return [1 / self.__unique_num * valid_ratio] * self.__unique_num
        elif self.__skew_pattern == "hotspot":
            if self.__unique_num == 3:
                rest_ratio = 0
            else:
                rest_ratio = (1 - self.__hotspot_first - self.__hotspot_second - self.__hotspot_third) / (self.__unique_num - 3)
            tmp_list = [self.__hotspot_first, self.__hotspot_second, self.__hotspot_third]
            tmp_list += [rest_ratio] * (self.__unique_num - 3)
            res_list = [x * valid_ratio for x in tmp_list]
            return res_list
        elif self.__skew_pattern == "gentle_zipf":  # gen zipf with all unique values
            # top3 values use the exactly correct status, the rest uses zipf
            top3_list = [self.__hotspot_first, self.__hotspot_second, self.__hotspot_third]
            top3_list = [x * valid_ratio for x in top3_list]
            rest_zipf_list = [1 / (i**self.__gentle_zipf_skew) for i in range(4, self.__unique_num + 1)]
            rest_sum_value = sum(rest_zipf_list)
            top3_sum_value = sum(top3_list)
            res_list = top3_list + (np.array(rest_zipf_list) / rest_sum_value * (valid_ratio - top3_sum_value)).tolist()
            return res_list
        else:
            print("Error: SKEW_PATTERN is not valid!", "SKEW_PATTERN:", self.__skew_pattern)
            exit(1)

    def __get_width_list(self, step_len, skewed_list):  # skewed_list is not changed in this function
        interval_list, width_prob_list = self.__preprocess_width(step_len)  # interval_list is always one element longer than prob_list
        # Get the width list following the distribution of width and skewness
        width_list = []
        possible_interval_range = len(interval_list) - 1
        occupied_prob_list = [0, 0, 0, 0, 0]
        # Reverse the skewed list, randomly choose a width-interval for this element.
        # If this width-interval is already full, turn to another interval.
        # TODO: the most repeated width in origin real data may not be the most one now.
        for rank_idx in range(0, len(skewed_list)):
            tmp_idx_list = list(range(0, possible_interval_range))
            self.myrandom.shuffle(tmp_idx_list)
            find_slot_flag = False
            for tmp_idx in tmp_idx_list:
                if occupied_prob_list[tmp_idx] + skewed_list[rank_idx] < width_prob_list[tmp_idx]:
                    find_slot_flag = True
                    break
            if not find_slot_flag:  # if there is no enough space for this element, put it in a not-full interval
                while (True):
                    random_idx = self.myrandom.choice(tmp_idx_list)
                    if occupied_prob_list[random_idx] < width_prob_list[random_idx]:
                        tmp_idx = random_idx
                        find_slot_flag = True
                        break
            # if (interval_list[tmp_idx] >= interval_list[tmp_idx + 1]):
            #     print("Get Width List Error: interval_lf: ", interval_list[tmp_idx], "interval_rh: ", interval_list[tmp_idx + 1])
            width_list.append(self.nprandom.randint(low=interval_list[tmp_idx], high=interval_list[tmp_idx + 1]))
            occupied_prob_list[tmp_idx] += skewed_list[rank_idx]
        return width_list

    def __cal_sortedness(self, input_list):
        # define larger and equal num to calculate sortedness
        larger_para = 0
        equal_para = 0
        tmp_value = input_list[0]
        tmp_len = len(input_list) + 1
        for ele in input_list:
            try:
                if ele > tmp_value:
                    larger_para += 1
                elif ele == tmp_value:
                    equal_para += 1
                tmp_value = ele  # update tmp_value for following comparison
            except Exception as e:
                print("row: {} ".format(ele), "tmp_value: {}".format(tmp_value), e)
        smaller_para = tmp_len - equal_para - larger_para - 1
        # if the sortedn_num is tmp_len/2, then it is quite no-monotonical, so we standardize it
        sorted_score = ((max(smaller_para, larger_para) + equal_para) - int(tmp_len / 2) + 1) / int(
            tmp_len / 2 + 0.5)  # the last 0.5 is for rounding to avoid sortedness > 1
        return sorted_score

    def __process_sortedness(self, full_data_list):
        res_data_list = []
        # sort_skew = SORT_SKEW
        # if (SORT_SCORE + sort_skew) > 1:
        #     sort_skew = 1 - SORT_SCORE
        # if (SORT_SCORE - sort_skew) < 0:
        #     sort_skew = SORT_SCORE
        for i in range(0, len(full_data_list), self.__sorted_window_size):
            tmp_list = full_data_list[i:i + self.__sorted_window_size]
            sorted_score = self.__cal_sortedness(tmp_list)
            # # 3 possible value: SORT_SCORE-sort_skew, SORT_SCORE, SORT_SCORE+sort_skew
            target_sort_socre = self.__sort_score
            # let the sortedness approach the target_sort_socre
            if sorted_score < target_sort_socre:
                tmp_list = sorted(full_data_list[i:i + self.__sorted_window_size])
                sorted_score = 1
            if sorted_score - target_sort_socre > 0.01:
                # now the sorted_score must be larger than target_sort_socre, so make it more unsorted
                # about unsorted size: (len/2 - unsorted_size) / (len/2) = sort_socre
                unsorted_size = int(1 / 2 * len(tmp_list) * (sorted_score - target_sort_socre))
                swap_idx = sorted(self.nprandom.choice(range(0, len(tmp_list)), size=unsorted_size * 2,
                                                   replace=False))  # unsorted_size*2 to make sure enough unsorted
                loop_cnt = 0
                for i in range(0, len(swap_idx)):
                    # swap the first and last element according to swap_idx, to make some disorder
                    lh = swap_idx[i]
                    rh = swap_idx[len(swap_idx) - i - 1]
                    tmp_list[lh], tmp_list[rh] = tmp_list[rh], tmp_list[lh]
                    loop_cnt += 1
                    if loop_cnt >= len(swap_idx) // 6 + 1:  # 6 is the hyper-para
                        loop_cnt = 0
                        sorted_score = self.__cal_sortedness(tmp_list)
                        if sorted_score - target_sort_socre < 0.01:
                            break
                # if swap cannot make disorder, try to use shuffle
                sorted_score = self.__cal_sortedness(tmp_list)
                if sorted_score - target_sort_socre > 0.01:
                    tmp_list_shuffle = tmp_list.copy()
                    self.myrandom.shuffle(tmp_list_shuffle)
                    shuffled_sorted_score = self.__cal_sortedness(tmp_list_shuffle)
                    # if shuffle approach the target_sort_socre twice better, then use it; 2 is the hyper-para
                    if abs(shuffled_sorted_score - target_sort_socre) * 2 < abs(sorted_score - target_sort_socre):
                        tmp_list = tmp_list_shuffle
            res_data_list += tmp_list
        return res_data_list

    def __get_full_data_list(self, null_format, unique_list, skewed_list):
        block_list = []  # each block should be a list
        # deal with the null value
        NULL_BLOCK_SIZE = 256
        NULL_BLOCK_NUM = int(self.__row_num * self.__null_ratio // NULL_BLOCK_SIZE)
        null_block = [null_format] * NULL_BLOCK_SIZE
        # append null blocks to the block list
        block_list += [null_block for _ in range(NULL_BLOCK_NUM)]

        # deal with the unique value
        remained_val_num_list = [int(self.__row_num * skew + 0.5) for skew in skewed_list]
        reversed_repeat_num_cut = self.__repeat_num_cut[::-1]
        adopted_num_list = []
        for idx in range(len(reversed_repeat_num_cut) - 1):
            adopted_num = (reversed_repeat_num_cut[idx + 1] + reversed_repeat_num_cut[idx] + 1) // 2  # (0,1] -> 1
            adopted_num_list.append(adopted_num)

        remained_block_num_list = self.__repeat_num_list[::-1]  # the number of 512-blocks, 400-blocks ....
        for block_idx in range(len(remained_block_num_list) - 1):  # don't need to deal with the 1-blocks
            # print("remained_block_num_list: ", remained_block_num_list)
            # print("remained_val_num_list: ", remained_val_num_list[:10])
            adopted_num = adopted_num_list[block_idx]
            filling_val_idx_list = list(range(len(remained_val_num_list)))
            # self.myrandom.shuffle(filling_val_idx_list)
            for val_idx in filling_val_idx_list:
                while (remained_val_num_list[val_idx] >= adopted_num and remained_block_num_list[block_idx] > 0):
                    remained_val_num_list[val_idx] -= adopted_num
                    block_list.append([unique_list[val_idx]] * adopted_num)  # add the value_block
                    remained_block_num_list[block_idx] -= 1
            # after the traverse, if there are still remained_blocks, we should split it to the next level
            if remained_block_num_list[block_idx] > 0:
                if block_idx < (len(remained_block_num_list) - 1):
                    # split_factor = adopted_num // adopted_num_list[block_idx + 1] / 2  # this 2 is just a empirical parameter
                    split_factor = 1
                    remained_block_num_list[block_idx + 1] += split_factor * remained_block_num_list[block_idx]
                    remained_block_num_list[block_idx] = 0

        # there are some remained values which are not filled in the above blocks
        remained_data_list = []
        for val_idx in range(len(remained_val_num_list)):
            if remained_val_num_list[val_idx] > 0:
                remained_data_list += [unique_list[val_idx]] * remained_val_num_list[val_idx]
        # print("[Debug] remained_data_list len: ", len(remained_data_list))
        # split the remained_data_list to the serval blocks of 256
        self.myrandom.shuffle(remained_data_list)
        tmp_block_size = 1
        remained_block_list = [remained_data_list[i:i + tmp_block_size] for i in range(0, len(remained_data_list), tmp_block_size)]

        # add the remained blocks to the block_list
        block_list += remained_block_list
        self.myrandom.shuffle(block_list)
        full_data_list = [item for sublist in block_list for item in sublist]
        # print(f"[Debug] After add remained value, full_data_list:{len(full_data_list)}, row_num:{self.__row_num}")

        # make the full_data_list to the length of row_num
        if len(full_data_list) >= self.__row_num:
            full_data_list = full_data_list[:self.__row_num]
        elif len(unique_list) == 0:
            full_data_list += [null_format] * (self.__row_num - len(full_data_list))
        else:
            full_data_list += self.nprandom.choice(unique_list[:10000], size=self.__row_num - len(full_data_list), replace=True).tolist()
        return full_data_list

    def __gen_int_data(self):
        # Some preparation about width and skewness
        INT_STEP = 10
        skewed_list = self.__get_skewed_list()
        width_list = self.__get_width_list(INT_STEP, skewed_list)

        # Generate the unique string list
        unique_list = []
        unique_list_set = set()
        max_val = None
        warning_values = 0
        if len(width_list) != self.__unique_num or len(width_list) != len(skewed_list):
            print("Length Error: width_list:", len(width_list), "skewed_list:", skewed_list, "self.__unique_num:", self.__unique_num)
            exit(1)
        while len(unique_list) < self.__unique_num:
            tmp_int = width_list[len(unique_list)]  # the width of int is just the value
            collision_cnt = 0
            update_step = 10
            while tmp_int in unique_list_set:
                tmp_int += self.nprandom.randint(0, update_step)
                collision_cnt += 1
                if collision_cnt > 20:
                    update_step *= 10
                    collision_cnt = 0
                if update_step > 1000:
                    warning_values += 1
                    if max_val == None:
                        tmp_int = max(unique_list_set) + 1
                    else:
                        tmp_int = max_val + 1
                    max_val = tmp_int
            unique_list.append(int(tmp_int))
            unique_list_set.add(int(tmp_int))
        if warning_values > 0:
            print("🤔 INFO: too many collisions in int_unique_list, {} values just use (max_value + 1)!".format(warning_values))

        # Extend the repeated_num with skewed list
        full_data_list = self.__get_full_data_list(nan, unique_list, skewed_list)

        # return sorted list
        res_list = self.__process_sortedness(full_data_list)
        return res_list

    def __gen_float_data(self):
        # float only has fixed width presentation
        FLOAT_DECIMAL_PLACE = 4
        skewed_list = self.__get_skewed_list()
        max_value = self.__unique_num * 100

        # Generate the unique float list
        unique_list = []
        unique_list_set = set()
        if self.__unique_num != len(skewed_list):
            print("Length Error:", "skewed_list:", skewed_list, "UNIQUE_NUM:", self.__unique_num)
            exit(1)
        while len(unique_list) < self.__unique_num:
            tmp_float = np.round(self.myrandom.uniform(0, max_value), FLOAT_DECIMAL_PLACE)
            while tmp_float in unique_list_set:
                tmp_float = np.round(self.myrandom.uniform(0, max_value), FLOAT_DECIMAL_PLACE)
            unique_list.append(tmp_float)
            unique_list_set.add(tmp_float)

        # Extend the repeated_num with skewed list
        full_data_list = self.__get_full_data_list(nan, unique_list, skewed_list)

        # return sorted list
        res_list = self.__process_sortedness(full_data_list)
        return res_list

    def __gen_string_data(self):
        # Some preparation about width and skewness
        STRING_STEP = 2
        skewed_list = self.__get_skewed_list()
        width_list = self.__get_width_list(STRING_STEP, skewed_list)

        # Generate the unique string list
        begin_time = time.time()
        unique_list = []
        unique_list_set = set()
        if len(width_list) != self.__unique_num or len(width_list) != len(skewed_list):
            print("Error: width_list and skewed_list  length is not equal to UNIQUE_NUM.")
            print("width_list:", len(width_list), "skewed_list:", skewed_list, "UNIQUE_NUM:", self.__unique_num)
            exit(1)
        collision_cnt = 0
        width_list_index = 0
        while len(unique_list) < self.__unique_num:
            tmp_str = ''.join(
                self.myrandom.choice(string.ascii_letters + string.digits) for _ in range(width_list[width_list_index % len(width_list)]))
            if tmp_str not in unique_list_set:
                collision_cnt = 0
                width_list_index += 1
                unique_list_set.add(tmp_str)
                unique_list.append(tmp_str)
            else:
                collision_cnt += 1
                if collision_cnt >= 20:
                    collision_cnt = 0
                    width_list_index += 1
                    # in case that all the width is 2 due to 0.99999 width_skew.
                    width_list[width_list_index % len(width_list)] += 1
        if (time.time() - begin_time > 20):
            print("🤔 INFO: too many collisions in string_unique_list, generating time:{}!".format(time.time() - begin_time))

        # Extend the repeated_num with skewed list
        full_data_list = self.__get_full_data_list('null', unique_list, skewed_list)

        # return sorted list
        res_list = self.__process_sortedness(full_data_list)
        return res_list

    def __gen_bool_data(self):
        # float only has fixed width presentation
        skewed_list = self.__get_skewed_list()

        # Generate the unique float list
        tmp_rand = self.nprandom.randint(0, 2)
        if tmp_rand == 0:
            unique_list = [True, False]
        else:
            unique_list = [False, True]

        # Extend the repeated_num with skewed list
        full_data_list = []
        for idx in range(0, self.__unique_num):
            repeated_num = int(self.__row_num * skewed_list[idx] + 0.5 - 1)  # plus 0.5 to round the number and get the closest integer
            if repeated_num == 0:
                break
            tmp_list = [unique_list[idx]] * repeated_num
            full_data_list += tmp_list

        if (len(full_data_list) < self.__row_num):  # if still not enough, we should random choose some  and add them
            tmp_list = self.myrandom.choices(unique_list, k=self.__row_num - len(full_data_list))
            full_data_list += tmp_list
        full_data_list = full_data_list[:self.__row_num]

        # shuffle the list, then return sorted list
        # the sortedness of bool is always quite high because it's hard to lower it down.
        self.myrandom.shuffle(full_data_list)
        return self.__process_sortedness(full_data_list)

    def gen_data(self) -> list:
        res_data = None
        if self.__data_type == "int":
            res_data = self.__gen_int_data()
        elif self.__data_type == "float":
            res_data = self.__gen_float_data()
        elif self.__data_type == "string":
            res_data = self.__gen_string_data()
        elif self.__data_type == "bool":
            res_data = self.__gen_bool_data()
        else:
            print("Error: data type not supported.")
            exit(1)
        return res_data
