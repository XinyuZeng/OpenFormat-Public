import statistics
from pyarrow import csv
import sys
import os
import json

def main():
	table_name = sys.argv[1]
	csv_name = os.path.join('tables', '%s.csv') % table_name
	file = open(csv_name[:-4])
	attribute = [line.strip('\n') for line in file.readlines()]
	file.close()
	print('---------- Begin reading csv ----------')
	table = csv.read_csv(csv_name,
		read_options=csv.ReadOptions(column_names=attribute),
		parse_options=csv.ParseOptions(delimiter='|'))
	statistics = {}
	row_size = table.shape[0]
	column_size = table.shape[1]
	print('---------- Begin survey ----------')
	for column_index in range(column_size):
		print('Begin column %d in %d columns' % (column_index, column_size))
		column_name = table.column_names[column_index]
		statistics[column_name] = {}
		seen_values = set()
		for index in range(row_size):
			if table[column_index][index] not in seen_values:
				statistics[column_name][table[column_index][index]] = 1
				seen_values.add(table[column_index][index])
			else:
				statistics[column_name][table[column_index][index]] += 1
	statistics_file = open("tables/%s.json" % table_name, 'x')
	statistics_file.write(json.dumps(statistics))
	statistics_file.close()
	from IPython import embed; embed()

if __name__ == '__main__':
	main()