# NOTICE: This script should be executed in public_bi_benchmark/benchmark,
# with directory 'tables' established.
# The repository is at git@github.com:cwida/public_bi_benchmark.git

import os

def main():
	tables = os.listdir('.')
	for table_name in tables:
		if table_name.endswith('.py') or table_name == 'tables':
			continue
		current_directory = '%s/tables' % table_name
		sql_files = os.listdir(current_directory)
		for file_name in sql_files:
			sql_file = open(os.path.join(current_directory, file_name))
			lines = sql_file.readlines()
			sql_file.close()
			output_file = open('tables/%s' % file_name[:-10], 'xt')
			for index in range(1, len(lines) - 1):
				line = lines[index].strip()
				right_index = line.find('\"', 1)
				if index != 1:
					output_file.write('\n')
				output_file.write(line[1:right_index])
			output_file.close()

if __name__ == '__main__':
	main()