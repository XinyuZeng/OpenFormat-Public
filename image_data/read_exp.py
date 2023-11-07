import pyarrow.parquet as pq
import pyarrow.orc as po
import pyarrow
import pandas as pd
import os
import requests
import time
import gc

# time.sleep(5)

l4_filter = [('NSFW', '=', 'NSFW'), ('similarity', '>', 0.42)]
l3_filter = [('NSFW', '=', 'NSFW'), ('similarity', '>', 0.365)]
l2_filter = [('NSFW', '=', 'NSFW'), ('similarity', '>', 0.31)]
l1_filter = [('similarity', '>', 0.368)]
l0_filter = None

l_filters = [l0_filter, l1_filter, l2_filter, l3_filter, l4_filter]
rg_size_list = [16, 64, 256, 1024, 4096, 16384, 65536, 262144]
selected_columns = [
    'key', 'caption', 'similarity', 'width', 'height', 'original_width', 'original_height', 'status', 'NSFW',
    'exif'
]

res_list = []
# for cols in [selected_columns]:
for cols in [None, selected_columns]:
    for l_idx in range(len(l_filters)):
        # for rg in [1024]:
        for rg in rg_size_list:
            for i in range(3):
                pqt_name = "./exp/final_0000_rg_%d.parquet" % rg
                os.system("echo 3 > /proc/sys/vm/drop_caches ")
                begin = time.time()
                pq_tbl = pq.read_table(pqt_name,
                                    columns=cols,
                                    filters=l_filters[l_idx],
                                    pre_buffer=False,
                                    use_threads=False)
                duration = time.time() - begin
                print(f"selectivity: {pq_tbl.num_rows / 219000}" )
                # print("Read time: ", duration)
                # log the duration
                if cols:
                    res_dict = {'col': 'tab', 'filter_level': l_idx, 'rg_size': rg, 'read_time': duration, 'i': i}
                else:
                    res_dict = {'col': 'complete', 'filter_level': l_idx, 'rg_size': rg, 'read_time': duration, 'i': i}
                print(res_dict)
                res_list.append(res_dict)
                pq_tbl = None  # gc manually
                gc.collect()

df = pd.DataFrame(res_list)
df.to_csv('output/final_read_time.csv', index=False)