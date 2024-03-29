import os, sys, pathlib
import numpy as np

dir_path = pathlib.Path(os.path.abspath('')).parent.resolve()
print(dir_path)
HOME_DIR = str(dir_path).split('/OpenFormat')[0]
PROJ_SRC_DIR = f'{HOME_DIR}/OpenFormat'
sys.path.insert(1, f'{PROJ_SRC_DIR}')
from python.scripts.utils import *

depth_list = [2, 4, 8, 17, 32, 47, 62]

common_exec = f'{HOME_DIR}/arrow-private/cpp/out/build/openformat-release/release/scan_to_arrow'
out_f = open('scan_time_arrow.txt', 'w')

for d in depth_list:
    pq_res_list = np.array([], dtype=np.int64)
    orc_res_list = np.array([], dtype=np.int64)
    for i in range(3):
        os.system('sync; echo 3 > /proc/sys/vm/drop_caches')
        res_lines = os.popen(f'{common_exec} {d}.parquet').read().split('\n')
        for line in res_lines:
            if line.startswith('read pq'):
                pq_time = float(line.split(':')[1].strip()) 
                break
        res_lines = os.popen(f'{common_exec} {d}.orc').read().split('\n')
        for line in res_lines:
            if line.startswith('read orc'):
                orc_time = float(line.split(':')[1].strip()) 
                break
        pq_res_list = np.append(pq_res_list, pq_time)
        orc_res_list = np.append(orc_res_list, orc_time)
    out_f.write(f'{pq_res_list.mean()} {orc_res_list.mean()}\n')
out_f.close()