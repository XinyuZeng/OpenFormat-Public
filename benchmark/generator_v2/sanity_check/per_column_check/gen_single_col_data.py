from cmath import isnan, nan
from math import sqrt
import string
import random
import time
import pandas as pd
import numpy as np
import json
import os
import sys

from generator import *

if __name__ == '__main__':

    tar_dir = "../../tmp_data/gen_data"
    config_file_name = "single_col_config.json"
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    os.makedirs(tar_dir, exist_ok=True)

    # read config file
    with open(config_file_name, 'r') as f:
        config = json.load(f)
    tar_file_name = config['basic_config']["target_file_name"]
    col_num = 1

    df_list = []
    for i in range(col_num):
        # create generator instance
        generator = Generator(config_file_name)
        res_data = generator.gen_data()
        # write to file
        # Attention!! If there is any null data in int column, pandas will convert the column to float
        res_df = pd.DataFrame(res_data)
        df_list.append(res_df)

    pd.concat(df_list, axis=1).to_csv(os.path.join(tar_dir, f'{tar_file_name}_gen.csv'), header=False, index=False)

    print(f"Generating Done: {tar_file_name}")
