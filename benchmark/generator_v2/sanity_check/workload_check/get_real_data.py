import sys
import numpy as np
import pandas as pd
import os
import json

N_ROWS = 500000
N_COLS = 20
dir_name = '/root/data/'
target_dir = "../../tmp_data/real_data/"

if __name__ == '__main__':

    if len(sys.argv) > 1:
        N_ROWS = int(sys.argv[1])
        N_COLS = int(sys.argv[2])

    stats_file_name = "../../../feature/outputs/stats.csv"
    total_status_df = pd.read_csv(stats_file_name, sep=',')
    workload_list = ['classic', 'geo', 'log', 'ml']
    # workload_list = ['classic']
    workload_dir_dict = {
        'classic': ['imdb', 'yelp', 'UKPP', 'menu'],
        'geo': ['cells', 'geo', 'flight'],
        'log': ['edgar', 'mgbench'],
        'ml': ['ml']
    }
    dtype_dict = {}
    for workload in workload_list:
        workload_stats_df = total_status_df[total_status_df['dataset'].isin(workload_dir_dict[workload])]
        workload_avail_cols = len(workload_stats_df)
        final_col_num = N_COLS
        if workload_avail_cols < N_COLS:
            # print('Not enough columns for workload: %s, just use full columns' % workload)
            final_col_num = workload_avail_cols
        df_list = []
        for dataset in workload_dir_dict[workload]:
            tables = os.listdir(os.path.join(dir_name, dataset))
            for table in tables:
                # read dataframe with different sep
                if dataset in ['geo', 'imdb']:
                    table_df = pd.read_csv(os.path.join(dir_name, dataset, table),
                                           sep='\t',
                                           header=None,
                                           skiprows=1,
                                           on_bad_lines='skip',
                                           nrows=N_ROWS,
                                           low_memory=False)
                else:
                    table_df = pd.read_csv(os.path.join(dir_name, dataset, table),
                                           sep=',',
                                           header=None,
                                           skiprows=1,
                                           on_bad_lines='skip',
                                           nrows=N_ROWS,
                                           low_memory=False)
                # print the rows of table_df if less than N_ROWS
                if len(table_df) < N_ROWS:
                    print(f'Not enough rows for table: {table}, just use full rows {len(table_df)}')
                # TODO: The logic can be improved without storing all the dataframes
                df_list.append(table_df)
        # concat all the dataframes
        concat_df = pd.concat(df_list, axis=1)
        # random pick final_col_num columns
        res_df = concat_df.sample(n=final_col_num, axis=1)
        # print datatypes
        dtype_status = res_df.dtypes.value_counts()
        # save col_num of dtypes to dict
        tmp_dict = {}
        map_dict = {np.dtype('int64'): 'int', np.dtype('float64'): 'float', np.dtype('object'): 'string'}
        has_bool = False
        for i in dtype_status.index:
            if i == np.dtype('bool'):
                has_bool = True
            else:
                tmp_dict[map_dict[i]] = int(dtype_status[i])
        tmp_dict['int'] = int(int(tmp_dict['int']) + has_bool)  # the number of bool col is no more than 1
        dtype_dict[workload] = tmp_dict

        # save
        res_df.to_csv(os.path.join(target_dir, workload + '_real.csv'), sep='\t', header=False, index=False)
        print('Collect real data, finish workload: %s 🎉' % workload)

    # write dtype dict to json file
    with open(os.path.join('./', 'real_data_type.json'), 'w') as f:
        json.dump(dtype_dict, f)
