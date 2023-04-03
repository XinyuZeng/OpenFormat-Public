import pandas as pd
import json
import yaml
import sys,os

# assume count_df is sorted by index
# Note: n^2 complexity now, can be improved later
def find_range(count_df, expectation, error, state):
    # return (val1, val2, selectivity, state)
    # if no range found, return None
    s = state['cur_sum']
    for l in range(state['l'], len(count_df)):
        right_bound = state['r']+1 if state['r']+1 < len(count_df) else l
        for r in range(max(l, right_bound), len(count_df)):
            s += count_df.values[r]
            if s >= expectation - error and s <= expectation + error:
                state['l'] = l
                state['r'] = r
                state['cur_sum'] = s
                return (count_df.index[l], count_df.index[r], s, state)
            elif s > expectation + error:
                break
        s = 0
    return None

def gen_filters_new(table_df, fea_dict, config, designated_idx=None):
    
    # set config to default from workload first
    # workload = config.get('workload')
    # default_config = yaml.safe_load(open(f'./filter_config/{workload}_filter.yaml'))
    # expectation = default_config['filter_config'].get('selectivity_expectation')
    # error = default_config['filter_config'].get('selectivity_error')
    # num_filters = default_config['filter_config'].get('num_filters')
    # point_portion = default_config['filter_config'].get('point_portion')
    # skew_pattern = default_config['filter_config'].get('skew_pattern')
    
    # set config to include new config
    expectation = config.get('selectivity_expectation')
    error = config.get('selectivity_error')
    num_filters = config.get('num_filters')
    point_portion = config.get('point_portion')
    if (expectation and error and num_filters and point_portion) is None:
        raise ValueError('Invalid filter config')
    skew_pattern = config.get('skew_pattern')
    sortness_threshold = config.get('sortness_threshold')
    data_type = config.get('data_type')
    
    # iterate df columns to prepare for filter generate
    point_satisified_cols = [] # type: Series
    count_df_list_sorted = []
    for (index, column) in enumerate(table_df):
        if designated_idx != None and index != designated_idx:
            continue
        if skew_pattern != None and skew_pattern != fea_dict[f'col{index}']['skew_fea']['skew_pattern']:
            continue
        if sortness_threshold != None and sortness_threshold > fea_dict[f'col{index}']['sort_fea']['sort_score']:
            continue
        if data_type != None:
            if data_type != fea_dict[f'col{index}']['basic_config']['data_type']:
                continue
        # else: 
        #     # CAUTION: we currently does not allow string filter because the implementation overhead on Parquet-rs
        #     if fea_dict[f'col{index}']['basic_config']['data_type'] == 'string':
        #         continue
        count_df = table_df[column].value_counts(normalize=True)
        count_df_list_sorted.append(count_df.sort_index())
        res = count_df[count_df.between(expectation-error, expectation+error)]
        if res.size > 0:
            point_satisified_cols.append(res)
    # print(count_df_list_sorted)
    # generate point filters in round robin fashion
    # save (col_idx, value, selectivty)
    point_results = []
    col_tracking = 0
    for i in range(round(num_filters * point_portion)):
        if len(point_satisified_cols) == 0:
            break
        res = (point_satisified_cols[col_tracking].name, fea_dict[f'col{point_satisified_cols[col_tracking].name}']['basic_config']['data_type'],
               point_satisified_cols[col_tracking].index[0], point_satisified_cols[col_tracking].values[0])
        point_satisified_cols[col_tracking] = point_satisified_cols[col_tracking].iloc[1:]
        if point_satisified_cols[col_tracking].size == 0:
            point_satisified_cols.pop(col_tracking)
        else:
            col_tracking += 1
        col_tracking %= len(point_satisified_cols) if point_satisified_cols else 1
        point_results.append(res)
    # print(point_results)
    
    # generate range filters in round robin fashion
    # assume range is [val1, val2], inclusive
    # save (col_idx, val1, val2, selectivty)
    range_results = []
    col_tracking = 0
    states = [{'l':0, 'r':-1, 'cur_sum': 0} for df in count_df_list_sorted]
    for i in range(round(num_filters * (1 - point_portion))):
        if len(count_df_list_sorted) == 0:
            break
        res = find_range(count_df_list_sorted[col_tracking], expectation, error, states[col_tracking])
        if res == None:
            count_df_list_sorted.pop(col_tracking)
            states.pop(col_tracking)
        else:
            states[col_tracking] = res[3]
            range_results.append((count_df_list_sorted[col_tracking].name, fea_dict[f'col{count_df_list_sorted[col_tracking].name}']['basic_config']['data_type'],
                                  res[0], res[1], res[2]))
            col_tracking += 1
        col_tracking %= len(count_df_list_sorted) if count_df_list_sorted else 1
    if len(point_results) == 0:
        print('No satisfied pointer filters')
    else:
        if os.path.exists(f"point_{expectation}.csv"):
            pd.DataFrame(point_results).to_csv(f"point_{expectation}.csv", header=None, index=False, mode='a')
        else:   
            pd.DataFrame(point_results).to_csv(f"point_{expectation}.csv", header=['col_idx', 'dtype', 'value', 'selectivity'], index=False)
    if len(range_results) == 0:
        print('No satisfied range filters')
    else:
        if os.path.exists(f"range_{expectation}.csv"):
            pd.DataFrame(range_results).to_csv(f"range_{expectation}.csv", header=None, index=False, mode='a')
        else:
            pd.DataFrame(range_results).to_csv(f"range_{expectation}.csv", header=['col_idx', 'dtype', 'val1', 'val2', 'selectivity'], index=False)


# main
# Usage: python3 filter_generator.py <csv_file> <fea_file> <config_file>
if __name__ == '__main__':
    csv_name = sys.argv[1]
    fea_name = sys.argv[2]
    config_name = sys.argv[3]
    designated_idx = int(sys.argv[4]) if len(sys.argv) > 4 else None
    config = yaml.safe_load(open(config_name))
    if 'filter_config' not in config:
        print('ERROR: no filter config in config file')
        exit(1)
    config_list = config['filter_config']

    df = pd.read_csv(csv_name, header=None)
    fea_dict = json.loads(open(fea_name, 'r').read())
    
    for i, c in enumerate(config_list):
        gen_filters_new(df, fea_dict, c, designated_idx)