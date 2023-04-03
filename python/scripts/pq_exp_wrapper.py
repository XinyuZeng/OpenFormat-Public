import pyarrow.orc as po
import pyarrow as pa
from pyarrow import csv
import pyarrow.dataset as ds
import sys
import time
import os
from utils import parse_config, enumerate_config, parse_output, collect_results

# python3 scripts/parquet_exp.py table_name(e.g Generico_1)
# experiment_name(e.g pq_test) [-w=true|false] [-r=true|false]
# [-o=true|false] [-c=column_name] [-manycsv=true|false]
if __name__ == "__main__":
    name = sys.argv[1]
    exp_name = sys.argv[2]
    os.system(
        f'python3 scripts/parquet_exp.py {name} {exp_name} -r=false -o=false')
    print('----Finished writing ORC file----')
    os.system('sync; echo 3 > /proc/sys/vm/drop_caches # drop mem cache')
    time.sleep(3)
    os.system(f'python3 scripts/parquet_read.py {name} {exp_name}')
