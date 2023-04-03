import json
import os
import sys
import math
import datetime
import pathlib
import pandas as pd

dir_path = pathlib.Path(os.path.abspath('')).resolve()
print(dir_path)
HOME_DIR = str(dir_path).split('/OpenFormat')[0]

timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

PROJ_SRC_DIR = f'{HOME_DIR}/OpenFormat'
sys.path.insert(1, f'{PROJ_SRC_DIR}')
from python.scripts.utils import *
os.chdir(f'{PROJ_SRC_DIR}/python')
df_list = []
name_list = ['encoding_size_sort_20230220_120213', 'encoding_size_width_20230220_151904',
             'encoding_size_zipf_20230221_033540', 'encoding_size_car_20230222_111503']
for n in name_list:
    df = pd.read_csv('outputs/'+n+'.csv') 
    df_list.append(df)
df_concat = pd.concat(df_list, ignore_index=True)
df_concat.to_csv("outputs/encoding_size_concat.csv", index=False)