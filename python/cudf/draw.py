from typing import List, Tuple, Dict, Set, Callable
from matplotlib import pyplot as plt
import pandas as pd
import json
import sys

file_types = ['pq', 'orc']
libs = ['cudf', 'pandas', 'pyarrow']
default_libs = ['cudf', 'pyarrow']
color_dict = {'pq': 'red', 'orc': 'blue'}
marker_dict = {'cudf': 'x', 'pyarrow': 'o', 'pandas': 'v'}
ls_dict = {
	True: '--', False: '-',
	'snappy': '--', 'none': '-', 'zstd': ':'
}
basic_color_set = {'pq_size', 'orc_size', 'pq_num_row_groups', 'orc_num_stripes'}
basic_color_dict = {'pq_size': 'red', 'orc_size': 'blue', 'pq_num_row_groups': 'red', 'orc_num_stripes': 'blue'}

def get_data(setting: Dict, time_s: float, property: str):
	if property == 'time_(s)':
		return time_s
	elif property == 'throughput':
		return setting['row_count'] / time_s
	else:
		return setting[property]

def update_single(prev, value, min_or_max: Callable):
	if prev is None:
		return value
	else:
		return min_or_max(prev, value)

def update_minmax(xy: Tuple, x, y):
	return (update_single(xy[0], x, min), update_single(xy[1], x, max),
		update_single(xy[2], y, min), update_single(xy[3], y, max))

def draw(settings: Dict[str, Dict], result: Dict[Tuple[str, str], Dict[str, float]], line: List[str], name: str):
	assert(len(line) >= 4)
	if line[0] == '*':
		draw_files = file_types
	else:
		assert(line[0] in file_types)
		draw_files = [line[0]]
	if line[1] == '*':
		draw_libs = default_libs
	else:
		assert(line[1] in libs)
		draw_libs = [line[1]]
	if len(line) > 4 and line[4] != '_':
		extra_key = line[4]
	else:
		extra_key = ''
	if len(line) > 5:
		suffixes = line[5].split('|')
	else:
		suffixes = ['']

	curves: Dict[Tuple[str, str], Dict[object, Tuple[List, List]]] = {}
	errors: Dict[Tuple[str, str], Dict[object, Tuple[List, List[Tuple]]]] = {}
	x_min = None
	x_max = None
	y_min = None
	y_max = None
	for file_type in draw_files:
		for lib in draw_libs:
			key = (file_type, lib)
			data_dict: Dict[object, Tuple[List, List]] = {}
			error_dict: Dict[object, Tuple[List, List]] = {}
			curve_set = set()
			for file_name in result[key]:
				file_name_selected = False
				for suffix in suffixes:
					if file_name.startswith(suffix):
						file_name_selected = True
				if not file_name_selected:
					continue
				x = get_data(settings[file_name], result[key][file_name][0], line[2])
				y = get_data(settings[file_name], result[key][file_name][0], line[3])
				y_low = get_data(settings[file_name], result[key][file_name][1], line[3])
				y_high = get_data(settings[file_name], result[key][file_name][2], line[3])
				x_min, x_max, y_min, y_max = update_minmax((x_min, x_max, y_min, y_max), x, y)
				if extra_key != '':
					curve_key = settings[file_name][extra_key]
				else:
					curve_key = None
				if curve_key not in curve_set:
					curve_set.add(curve_key)
					data_dict[curve_key] = ([], [])
					error_dict[curve_key] = ([], [])
				data_dict[curve_key][0].append(x)
				data_dict[curve_key][1].append(y)
				y_range = [y, y_low, y_high]
				y_range.sort()
				error_dict[curve_key][0].append(y_range[1] - y_range[0])
				error_dict[curve_key][1].append(y_range[2] - y_range[1])
			curves[key] = data_dict
			errors[key] = error_dict
	
	# Version w/o an error bar
	if x_min > 0 and x_max / x_min > 100:
		plt.xscale('log', base=2)
	if y_min > 0 and y_max / y_min > 100:
		plt.yscale('log')
	plt.xlabel(line[2])
	plt.ylabel(line[3])
	for key in curves:
		for extra_key_value in curves[key]:
			if extra_key == '':
				suffix = ''
			else:
				suffix = ', %s=%s' % (extra_key, extra_key_value)
			arg_dict = {}
			if len(extra_key) != 0:
				arg_dict['ls'] = ls_dict[extra_key_value]
			# plt.errorbar(curves[key][extra_key_value][0], curves[key][extra_key_value][1], yerr=errors[key][extra_key_value],
			# 	label='%s, %s%s' % (key[0], key[1], suffix),
			# 	color=color_dict[key[0]], ecolor='purple', elinewidth=1, capsize=4, marker=marker_dict[key[1]], **arg_dict)
			plt.plot(curves[key][extra_key_value][0], curves[key][extra_key_value][1], label='%s, %s%s' % (key[0], key[1], suffix),
				color=color_dict[key[0]], marker=marker_dict[key[1]], **arg_dict)
	plt.legend()
	plt.savefig('figure/%s.png' % name, dpi=300)
	plt.clf()
	
	# Version with an error bar
	if x_min > 0 and x_max / x_min > 100:
		plt.xscale('log', base=2)
	if y_min > 0 and y_max / y_min > 100:
		plt.yscale('log')
	plt.xlabel(line[2])
	plt.ylabel(line[3])
	for key in curves:
		for extra_key_value in curves[key]:
			if extra_key == '':
				suffix = ''
			else:
				suffix = ', %s=%s' % (extra_key, extra_key_value)
			arg_dict = {}
			if len(extra_key) != 0:
				arg_dict['ls'] = ls_dict[extra_key_value]
			plt.errorbar(curves[key][extra_key_value][0], curves[key][extra_key_value][1], yerr=errors[key][extra_key_value],
				label='%s, %s%s' % (key[0], key[1], suffix),
				color=color_dict[key[0]], ecolor='purple', elinewidth=1, capsize=4, marker=marker_dict[key[1]], **arg_dict)
			# plt.plot(curves[key][extra_key_value][0], curves[key][extra_key_value][1], label='%s, %s%s' % (key[0], key[1], suffix),
			# 	color=color_dict[key[0]], marker=marker_dict[key[1]], **arg_dict)
	plt.legend()
	plt.savefig('figure/%s_ebar.png' % name, dpi=300)
	plt.clf()

def draw_basic(name: str, x_name: str, properties: List[str], settings: Dict[str, Dict], y_label: str=None):
	plt.xlabel(x_name)
	if y_label is None:
		assert(len(properties) == 1)
		y_label = properties[0]
	plt.ylabel(y_label)

	with open('experiment/%s.txt' % name, 'rt') as file:
		raw_file_names = [line.split()[0] for line in file.readlines()]
		for index, raw_file_name in enumerate(raw_file_names):
			if raw_file_name.endswith('.pq'):
				raw_file_names[index] = raw_file_name[:-3]
			elif raw_file_name.endswith('.orc'):
				raw_file_names[index] = raw_file_name[:-4]
	x_values = [settings[raw_file_name][x_name] for raw_file_name in raw_file_names]
	x_min = min(x_values)
	x_max = max(x_values)

	y_min = None
	y_max = None
	y_value_lists: List[Tuple[str, List]] = []
	for property in properties:
		y_value_lists.append((property, [settings[raw_file_name][property] for raw_file_name in raw_file_names]))
		y_new_min = min(y_value_lists[-1][1])
		y_new_max = max(y_value_lists[-1][1])
		x_min, x_max, y_min, y_max = update_minmax((x_min, x_max, y_min, y_max), x_min, y_new_min)
		x_min, x_max, y_min, y_max = update_minmax((x_min, x_max, y_min, y_max), x_min, y_new_max)

	if x_min > 0 and x_max / x_min > 100:
		plt.xscale('log', base=2)
	if y_min > 0 and y_max / y_min > 100:
		plt.yscale('log')
	
	for y_values in y_value_lists:
		arg_dict = {}
		if y_values[0] in basic_color_set:
			arg_dict['color'] = basic_color_dict[y_values[0]]
		plt.plot(x_values, y_values[1], label=y_values[0], **arg_dict)
	
	plt.legend()
	plt.savefig('figure/%s_%s.png' % (name, '_'.join(properties)), dpi=300)
	plt.clf()

def main():
	name = sys.argv[1]
	raw_result: Dict[Tuple[str, str], Dict[str, List[float]]] = {}
	raw_file_name_set_dict: Dict[Set[str]] = {}
	result_set: Set[Tuple[str, str]] = set()
	settings: Dict[str, Dict] = {}
	settings_set: Set[str] = set()
	
	result_csv = pd.read_csv('result/%s.csv' % name)
	for index in range(len(result_csv)):
		file_name = result_csv.loc[index, 'file']
		suffix = file_name.split('.')[-1]
		raw_file_name = file_name[:-len(suffix) - 1]
		assert(suffix in file_types)
		assert(result_csv.loc[index, 'lib'] in libs)
		if raw_file_name not in settings_set:
			settings_set.add(raw_file_name)
			settings[raw_file_name] = json.load(open('setting/%s' % raw_file_name, 'rt'))
		key = (suffix, result_csv.loc[index, 'lib'])
		if key not in result_set:
			result_set.add(key)
			raw_result[key] = {}
			raw_file_name_set_dict[key] = set()
		if raw_file_name not in raw_file_name_set_dict[key]:
			raw_file_name_set_dict[key].add(raw_file_name)
			raw_result[key][raw_file_name] = []
		raw_result[key][raw_file_name].append(result_csv.loc[index, 'time_(s)'])

	result: Dict[Tuple[str, str], Dict[str, Tuple[float, float, float]]] = {}
	for key in raw_result:
		result[key] = {}
		for raw_file_name in raw_result[key]:
			assert(len(raw_result[key][raw_file_name]) == 8)
			avg = sum(raw_result[key][raw_file_name]) / len(raw_result[key][raw_file_name])
			lowest = min(raw_result[key][raw_file_name])
			highest = max(raw_result[key][raw_file_name])
			result[key][raw_file_name] = (avg, lowest, highest)

	x_name: str = None
	with open('figure/%s.txt' % name, 'rt') as file:
		lines = [line.split() for line in file.readlines()]
	for index, line in enumerate(lines):
		if x_name is None:
			x_name = line[2]
		draw(settings, result, line, '%s_%d' % (name, index))
	
	assert(x_name is not None)
	draw_basic(name, x_name, ['row_count'], settings)
	draw_basic(name, x_name, ['pq_size', 'orc_size'], settings, 'file_size')
	draw_basic(name, x_name, ['pq_num_row_groups'], settings)
	draw_basic(name, x_name, ['orc_num_stripes'], settings)

if __name__ == '__main__':
	main()
