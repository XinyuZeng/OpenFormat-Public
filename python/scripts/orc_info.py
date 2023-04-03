import pyarrow.orc as po
import pandas as pd
import sys
from orc_exp import orc_info

file_name = sys.argv[1]

orc_info(file_name)
# pd.set_option('display.max_columns', None)

# df = pq.read_table(source=sys.argv[1]).to_pandas()

# print(df)
