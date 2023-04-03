import pandas as pd
import sys
import os

suffixes = ['gzip', 'lz4', 'zstd', 'none']

def main():
	table_name = sys.argv[1]
	if len(sys.argv) > 2:
		round = int(sys.argv[2])
	else:
		round = 3
	directory = os.path.join('result', 'compression_level', table_name)
	tables = []
	for index in range(round):
		for suffix in suffixes:
			table = pd.read_csv(os.path.join(directory, str(index), 'parquet_%s.csv' % suffix))
			table['round'] = [index] * len(table['compression'])
			tables.append(table)
	table = pd.concat(tables)
	table.sort_values(by=['round', 'compression', 'use_dictionary', 'compression_level'], inplace=True)
	table.index = list(range(int(table.shape[0] / round))) * round
	table.to_csv(os.path.join(directory, 'conclusion.csv'))

if __name__ == '__main__':
	main()