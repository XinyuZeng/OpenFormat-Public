import pyarrow.parquet as pq
import pandas as pd
import sys
from parquet_exp import parquet_info

file_name = sys.argv[1]

parquet_info(file_name)
# pd.set_option('display.max_columns', None)

# df = pq.read_table(source=sys.argv[1]).to_pandas()

