import numpy as np
import matplotlib.pyplot as plt
import sys
import os
from parse_io_profile import parse_io_file

cpu_numbers = [1, 2, 4, 8, 16, 24, 32]
# cpu_numbers = [1, 2, 4, 8]
columns = ['kB_read/s']
# columns = ['tps']
# columns = ['%user', '%nice', '%system', '%iowait', '%steal', '%idle']

def main():
	directory_name = sys.argv[1]
	result = {}
	for column in columns:
		result[column] = []
	for cpu_num in cpu_numbers:
		result_cpu, result_device, _  = parse_io_file(os.path.join(directory_name, 'io_%d00.log' % cpu_num))
		for column in columns:
			result[column].append(np.mean(result_device[column][1:]))
		# for column in columns:
		# 	result[column].append(np.mean(result_cpu[column][1:]))
	for column in columns:
		plt.plot(cpu_numbers, result[column], label=column)
	plt.legend()
	plt.savefig('draw_io_output.jpg')

if __name__ == '__main__':
	main()