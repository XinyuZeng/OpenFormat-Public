import numpy as np
import pandas as pd
import os
from write_read_exp import *

os.chdir(os.path.dirname(os.path.abspath(__file__)))
os.system("cd ../../tmp_data;rm -r ./real_data/ ./gen_data/  ./format_res/orc/ ./format_res/pqt/")
os.system("cd ../../tmp_data;mkdir -p ./real_data/ ./gen_data/  ./format_res/orc/ ./format_res/pqt/")

os.system("python get_real_data.py 256000 200;")
os.system("cd ../../; python gen_workloads.py multiple 256000 1 gen;")

gen_data_dir = '../../tmp_data/gen_data'
real_data_dir = '../../tmp_data/real_data'
format_res_dir = '../../tmp_data/format_res'
gen_files = os.listdir(gen_data_dir)
real_files = os.listdir(real_data_dir)
for gen_file in gen_files:
    base_gen_file = gen_file.split('.csv')[0]
    do_all_exp(os.path.join(gen_data_dir, gen_file), format_res_dir, base_gen_file)  # pay attention to the relative path
for real_file in real_files:
    base_real_file = real_file.split('.csv')[0]
    do_all_exp(os.path.join(real_data_dir, real_file), format_res_dir, base_real_file)

os.system("cd ../../; python get_data_status.py")

print("Workload sanity check done!")