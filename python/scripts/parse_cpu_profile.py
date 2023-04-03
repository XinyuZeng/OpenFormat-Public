import sys

column_names = ['%usr', '%nice', '%sys', '%iowait', '%irq', '%soft', '%steal', '%guest', '%gnice', '%idle']

def parse_cpu_line(result, line):
	columns = line.split()
	for index, column_name in enumerate(column_names):
		result[column_name].append(float(columns[index + 3]))

def parse_cpu_file(file_name):
	result = {}
	for column_name in column_names:
		result[column_name] = []
	with open(file_name, 'rt') as file:
		lines = file.readlines()
	for index in range(3, len(lines) - 1):
		parse_cpu_line(result, lines[index])
	return result

if __name__ == '__main__':
	profile_name = sys.argv[1]
	file_name = '%s.log' % profile_name
	result = parse_cpu_file(file_name)
	print(result)