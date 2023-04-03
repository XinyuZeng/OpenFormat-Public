import sys
import json
import os
import pandas as pd
import matplotlib.pyplot as plt

extra_column_names = ['write_2MB', 'write_20MB', 'write_200MB', 'compression_rate', 'compression_rate_csv']

# Adopted from scripts/utils.py
def enumerate_config(formats_dict):
    '''list type for each config in .json, and it will enumerate all 
    the possible combinations of the config. i.e. the number of tests 
    in total will be the product of the length of every list.'''
    args = [{}]
    for key in formats_dict:
        new_args = []
        if isinstance(formats_dict[key], list):
            for i, x in enumerate(formats_dict[key]):
                for arg in args:
                    new_arg = arg.copy()
                    new_arg[key] = x
                    new_args.append(new_arg)
        else:
            for arg in args:
                new_arg = arg.copy()
                new_arg[key] = formats_dict[key]
                new_args.append(new_arg)
        args = new_args
    return args

def update_column(conclusion, statistic, new_columns, column_name):
	if column_name not in new_columns and column_name in extra_column_names:
		new_columns.add(column_name)
		if column_name == 'write_2MB':
			conclusion["write_2MB"] = conclusion["write_parquet_time (s)"] + conclusion["file_size (MB)"] / 2
		elif column_name == 'write_20MB':
			conclusion["write_20MB"] = conclusion["write_parquet_time (s)"] + conclusion["file_size (MB)"] / 20
		elif column_name == 'write_200MB':
			conclusion["write_200MB"] = conclusion["write_parquet_time (s)"] + conclusion["file_size (MB)"] / 200
		elif column_name == 'compression_rate':
			conclusion['compression_rate'] = [line / statistic['parquet_dictionary_raw (MB)'] if conclusion['use_dictionary'].iloc[index] else line / statistic['parquet_no_dictionary_raw (MB)'] for index, line in enumerate(conclusion['file_size (MB)'])]
		elif column_name == 'compression_rate_csv':
			conclusion['compression_rate_csv'] = conclusion['file_size (MB)'] / statistic['csv_size (MB)']
		else:
			print('Unknown extra column: %s' % column_name)

def main():
	table_name = sys.argv[1]
	input_file_name = sys.argv[2]
	with open(input_file_name, 'rt') as file:
		format_files = [line.strip() for line in file.readlines()]
	table_path = os.path.join('result', 'compression_level', table_name)
	conclusion = pd.read_csv(os.path.join(table_path, 'conclusion.csv'), index_col=0)
	with open(os.path.join(table_path, 'statistic.json')) as file:
		statistic = json.load(file)
	parameter_set = 0
	while parameter_set != conclusion.shape[0]:
		if conclusion['round'].iloc[parameter_set] != 0:
			break
		parameter_set += 1
	new_columns = set()
	for format_file_name in format_files:
		with open(os.path.join('format', '%s.json' % format_file_name), 'r') as file:
			current_format = json.load(file)
		formats = enumerate_config(current_format)
		for format in formats:
			update_column(conclusion, statistic, new_columns, format["x"])
			update_column(conclusion, statistic, new_columns, format["y"])
			draw_parquet(conclusion, format, table_name, parameter_set)

def draw_parquet(conclusion, format, table_name, parameter_set):
	data_x = [[], [], [], [], [], []]
	data_y = [[], [], [], [], [], []]
	data_index = [[], [], [], [], [], []]
	index = 0
	while index != parameter_set:
		if conclusion["compression"].iloc[index] == format["compression"]:
			break
		index += 1
	while index != parameter_set:
		if conclusion["compression"].iloc[index] != format["compression"]:
			break
		if format["use_dictionary"] == "both" or format["use_dictionary"] == str(conclusion["use_dictionary"].iloc[index]):
			if format["round"] == "all":
				for round in range(3):
					data_x[int(conclusion["use_dictionary"].iloc[index]) * 3 + round].append(conclusion[format["x"]].iloc[round * parameter_set + index])
					data_y[int(conclusion["use_dictionary"].iloc[index]) * 3 + round].append(conclusion[format["y"]].iloc[round * parameter_set + index])
					data_index[int(conclusion["use_dictionary"].iloc[index]) * 3 + round].append(index)
			else:
				round = int(format["round"])
				data_x[int(conclusion["use_dictionary"].iloc[index]) * 3 + round].append(conclusion[format["x"]].iloc[round * parameter_set + index])
				data_y[int(conclusion["use_dictionary"].iloc[index]) * 3 + round].append(conclusion[format["y"]].iloc[round * parameter_set + index])
				data_index[int(conclusion["use_dictionary"].iloc[index]) * 3 + round].append(index)
		index += 1
	if 'log' in format.keys():
		if format['log'] == 'both':
			plt.axes(yscale='log', xscale='log')
		elif format['log'] == 'x':
			plt.axes(xscale='log')
		elif format['log'] == 'y':
			plt.axes(yscale='y')
	for index in range(6):
		if len(data_index[index]):
			plt.plot(data_x[index], data_y[index], label='%s-%d' % (str(bool(index > 2)), index % 3))
	plt.xlabel(format["x"])
	plt.ylabel(format["y"])
	plt.title(format["compression"])
	plt.legend()
	plt.savefig(os.path.join('image', '%s-%s-%s-%s-%s-%s-%s.jpg') % (table_name, format["x"], format["y"], format["round"], format["compression"], format["use_dictionary"], format["policy"]))
	plt.clf()

if __name__ == '__main__':
	main()