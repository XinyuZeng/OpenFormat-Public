import json
import sys
import os
import pandas as pd

def main():
	table_name = sys.argv[1]
	if len(sys.argv) > 2:
		round = int(sys.argv[2])
	else:
		round = 3
	statistic = {}
	csv_file_name = os.path.join('tables', '%s.csv' % table_name)
	statistic['csv_size (MB)'] = os.stat(csv_file_name).st_size / 1024 ** 2
	conclusion = pd.read_csv(os.path.join('result', 'compression_level', table_name, 'conclusion.csv'), index_col=0)
	for index in range(int(conclusion.shape[0] / round)):
		if round != 1:
			if conclusion["compression"][index].iloc[0] == 'NONE':
				if conclusion["use_dictionary"][index].iloc[0]:
					statistic['parquet_dictionary_raw (MB)'] = conclusion["file_size (MB)"][index].iloc[0]
				else:
					statistic['parquet_no_dictionary_raw (MB)'] = conclusion["file_size (MB)"][index].iloc[0]
		else:
			if conclusion["compression"][index] == 'NONE':
				if conclusion["use_dictionary"][index]:
					statistic['parquet_dictionary_raw (MB)'] = conclusion["file_size (MB)"][index]
				else:
					statistic['parquet_no_dictionary_raw (MB)'] = conclusion["file_size (MB)"][index]
	with open(os.path.join('result', 'compression_level', table_name, 'statistic.json'), 'x') as file:
		json.dump(statistic, file)

if __name__ == '__main__':
	main()