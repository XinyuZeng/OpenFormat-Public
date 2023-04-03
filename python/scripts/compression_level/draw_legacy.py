import sys
import os
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

total_group = ['0', '1', '2']

def main():
	if len(sys.argv) < 4:
		print('Insufficient arguments. For example, type \'python3 draw.py Generico_1 3 parquet_none definition\','
			'with file \'definition\' resides in the same directory, indicating the variables on which to draw.')
		sys.exit(1)
	table_name = sys.argv[1]
	table_directory = os.path.join('result', 'compression_level', table_name)
	if not os.path.exists(table_directory):
		print('Required result of table \'%s\' does not exist.' % table_name)
		sys.exit(2)
	file_name = '%s.csv' % sys.argv[3]
	if sys.argv[2] in total_group:
		group_set = [sys.argv[2]]
		if not os.path.exists(os.path.join(table_directory, sys.argv[2], file_name)):
			print('Unrecognized json file: \'%s\'.' % file_name)
			sys.exit(3)
	elif sys.argv[2] == 'x':
		group_set = total_group
		for group_name in total_group:
			if not os.path.exists(os.path.join(table_directory, group_name, file_name)):
				print('In group \'%s\', no result named \'%s\'' % (group_name, file_name))
				sys.exit(3)
	else:
		print('Unrecognized group number: \'%s\'.' % sys.argv[2])
		sys.exit(4)
	definition_file = sys.argv[4]
	if not os.path.exists(definition_file):
		print('The file \'%s\' does not exist. Please check and redo.' % definition_file)
		sys.exit(5)
	with open(definition_file, 'rt') as file:
		y_axis = file.readline().strip()
		plt.ylabel(y_axis)
		x_axis = file.readline().strip()
		plt.xlabel(x_axis)
	results = {}
	for group_name in group_set:
		results[group_name] = pd.read_csv(os.path.join(table_directory, group_name, file_name))
	dict_points = {}
	dict_points[y_axis] = []
	dict_points[x_axis] = []
	no_dict_points = {}
	no_dict_points[y_axis] = []
	no_dict_points[x_axis] = []
	group_length = len(group_set)
	index_translate = {}
	for group_index, group_name in enumerate(group_set):
		for index in range(len(results[group_set[0]][x_axis])):
			if group_index == 0:
				if results[group_name]['use_dictionary'][index]:
					dict_points[y_axis].append(results[group_name][y_axis][index] / group_length)
					dict_points[x_axis].append(results[group_name][x_axis][index] / group_length)
					index_translate[index] = len(dict_points[y_axis]) - 1
				else:
					no_dict_points[y_axis].append(results[group_name][y_axis][index] / group_length)
					no_dict_points[x_axis].append(results[group_name][x_axis][index] / group_length)
					index_translate[index] = len(no_dict_points[y_axis]) - 1
			else:
				if results[group_name]['use_dictionary'][index]:
					dict_points[y_axis][index_translate[index]] += results[group_name][y_axis][index] / group_length
					dict_points[x_axis][index_translate[index]] += results[group_name][x_axis][index] / group_length
				else:
					no_dict_points[y_axis][index_translate[index]] += results[group_name][y_axis][index] / group_length
					no_dict_points[x_axis][index_translate[index]] += results[group_name][x_axis][index] / group_length

	x = np.meshgrid(np.arange(0, 600, 1))
	y = np.meshgrid(np.arange(0, 600, 1))
	X, Y = np.meshgrid(x, y)
	Z = X + Y / 10
	plt.contourf(X, Y, Z)
	plt.contour(X, Y, Z)
	
	plt.scatter(dict_points[x_axis], dict_points[y_axis], c='blue', label='With dict')
	plt.scatter(no_dict_points[x_axis], no_dict_points[y_axis], c='red', label='No dict')


	plt.legend()
	plt.savefig('temp.jpg')

if __name__ == '__main__':
	main()