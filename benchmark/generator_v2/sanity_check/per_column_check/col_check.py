import numpy as np
import pandas as pd
import os
import sys
from write_read_exp import *

data_dir = '/root/data/'
tar_dir = '../../tmp_data'
dataset_list = ['imdb', 'yelp', 'UKPP', 'menu', 'cells', 'geo', 'flight', 'edgar', 'mgbench', 'ml']  # no cwi
dataset_list = ['imdb']
ROW_NUM = 100000

if __name__ == "__main__":

    os.system("cd ../../tmp_data;rm -r ./real_data/ ./gen_data/  ./format_res/orc/ ./format_res/pqt/")
    os.system("cd ../../tmp_data;mkdir -p ./real_data/ ./gen_data/  ./format_res/orc/ ./format_res/pqt/")

    for dataset in dataset_list:
        tables = os.listdir(os.path.join(data_dir, dataset))
        # tables = ['cell_towers.csv']
        for table_file_name in tables[:2]:
            csv_file_name = os.path.join(data_dir, dataset, table_file_name)
            if dataset in ['imdb']:
                table_base_name = table_file_name.split('.tsv')[0]
            else:
                table_base_name = table_file_name.split('.')[0]

            if dataset in ['edgar', 'ml', 'yelp', 'flight', 'cells', 'menu', 'mgbench', "UKPP"]:
                df = pd.read_csv(csv_file_name, sep=',', header=None, skiprows=1, on_bad_lines='warn', nrows=ROW_NUM, low_memory=False)
            elif dataset in ['geo', 'imdb']:
                df = pd.read_csv(csv_file_name, sep='\t', header=None, skiprows=1, on_bad_lines='warn', nrows=ROW_NUM, low_memory=False)
            elif dataset in ['cwi']:
                df = pd.read_csv(csv_file_name, sep='|', header=None, skiprows=1, on_bad_lines='warn', nrows=ROW_NUM, low_memory=False)

            col_num = len(df.columns)
            for col_idx in range(0, col_num):
                # for col_idx in [11]:
                tmp_df = df[col_idx]
                real_csv_file_name = os.path.join(tar_dir, 'real_data', f"{table_base_name}_{col_idx}.csv")
                tmp_df.to_csv(real_csv_file_name, index=False, header=False)

                # get the fea_config of this column
                adopted_row_num = min(ROW_NUM, len(tmp_df))
                os.system(f"python gen_single_col_config.py {table_file_name} {col_idx} {adopted_row_num}")
                # generate new colunm using the above status
                # the input config file is "single_col_config.json"
                # the output file is "{table_base_name}_{col_idx}_gen.csv"
                os.system("python gen_single_col_data.py")

                gen_csv_file_name = os.path.join(tar_dir, 'gen_data', f"{table_base_name}_{col_idx}_gen.csv")
                do_all_exp(real_csv_file_name, os.path.join(tar_dir, "format_res"), f"{table_base_name}_{col_idx}")
                do_all_exp(gen_csv_file_name, os.path.join(tar_dir, "format_res"), f"{table_base_name}_{col_idx}_gen")

    os.system("cd ../../;python get_data_status.py")

    # write the generated column to orc and parquet file
