# Rearrange parquet files
import pyarrow.parquet as pq
import pyarrow.orc as po
import pyarrow as pa
import pandas as pd
import os
import time
'''
# concatenate several pqt files
begin = time.time()
start_pqt_name="./exp/0000_rg_final_64k.parquet"
start_tbl=pq.read_table(start_pqt_name)
print("read pqt time (s):", (time.time() - begin))
pa_tbl=start_tbl
step=1000
for lh in range(0,372000,step):
    pqt_name="./pqt/0001_{}_dst.parquet".format(lh)
    # find whether the file exists
    if not os.path.exists(pqt_name):
        continue
    begin = time.time()
    tmp_table = pq.read_table(pqt_name)
    print("{}, read pqt time (s):".format(lh), time.time() - begin)
    pa_tbl = pa.concat_tables([pa_tbl, tmp_table])
'''

os.system("echo 3 > /proc/sys/vm/drop_caches ")

# Read from a single parquet file
pqt_name = "./final.parquet"
begin = time.time()
pa_tbl = pq.read_table(pqt_name)
duration = time.time() - begin
print("Read time: ", duration)

# write the final parquet file
selected_columns = [
    'key', 'caption', 'similarity', 'width', 'height', 'original_width', 'original_height', 'status', 'NSFW',
    'exif'
]
other_cols = ['url', 'text_embedding', 'image_embedding']
compression_dict = {}
for col in selected_columns:
    compression_dict[col] = 'SNAPPY'
for col in other_cols:
    compression_dict[col] = 'NONE'

rg_size_list = [16, 64, 256, 1024, 4096, 16384, 65536, 262144]
for rg in rg_size_list:
    dst_pqt_name = f"./exp/final_0000_rg_{rg}.parquet"
    parquet_config = {
        'version': '2.6',
        'use_dictionary': selected_columns,
        # 'compression': compression_dict,
        "row_group_size": rg,
        # 'compression_level': None,
        "dictionary_pagesize_limit": 1048576,
        'data_page_version': '2.0'
    }
    begin = time.time()
    pq.write_table(pa_tbl, dst_pqt_name, **parquet_config)
    print(rg, ", write pqt time (s):", time.time() - begin)
