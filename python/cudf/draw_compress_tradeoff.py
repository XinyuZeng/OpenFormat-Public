from typing import Dict, List, Set
from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
import sys
import pathlib
import os

from analyze_profile import analyze_csv

dir_path = pathlib.Path(os.path.abspath('')).parent.resolve()
HOME_DIR = str(dir_path).split('/OpenFormat')[0]
PROJ_SRC_DIR = f'{HOME_DIR}/OpenFormat'
sys.path.insert(1, f'{PROJ_SRC_DIR}')
from python.plot.stylelib import *

def str_to_row_count(row_count_str: str) -> int:
	if row_count_str.endswith('K'):
		return int(row_count_str[:-1]) * 1024
	else:
		assert(row_count_str.endswith('M'))
		return int(row_count_str[:-1]) * 1024 ** 2

def main():
	stats_dict: Dict[str, List[Dict]] = {}
	comp_types = ['none', 'zstd']
	for comp_index, compression in enumerate(comp_types):
		stats_list: List[Dict] = []
		for index in range(13):
			csv = pd.read_csv('ncu_csv/compress_orc_%d.csv' % ((comp_index + 1) * 13 + index))
			stats_list.append(analyze_csv(csv, 'dict'))
		stats_dict[compression] = stats_list

	y_values: Dict[str, List[float]] = {}

	# In milliseconds
	y_values['none_decode'] = []
	y_values['none_decomp'] = []
	y_values['zstd_decode'] = []
	y_values['zstd_decomp'] = []
	for index in range(13):
		none_set: Set[str] = set()
		none_dict: Dict[str, float] = {}
		none_decode = 0.0
		for kernel_name, duration, count in stats_dict['none'][index]['kernel_list']:
			none_set.add(kernel_name)
			none_dict[kernel_name] = (duration, count)
			none_decode += duration
		
		zstd_set: Set[str] = set()
		zstd_dict: Dict[str, float] = {}
		zstd_decode = 0.0
		zstd_decomp = 0.0
		for kernel_name, duration, count in stats_dict['zstd'][index]['kernel_list']:
			zstd_set.add(kernel_name)
			zstd_dict[kernel_name] = (duration, count)
			if kernel_name in none_set:
				zstd_decode += duration
			else:
				zstd_decomp += duration
		
		for none_kernel in none_set:
			assert(none_kernel in zstd_set)
			assert(none_dict[none_kernel][1] == zstd_dict[none_kernel][1])

		y_values['none_decode'].append(none_decode / 1000)
		y_values['none_decomp'].append(0.0)
		y_values['zstd_decode'].append(zstd_decode / 1000)
		y_values['zstd_decomp'].append(zstd_decomp / 1000)

	x_values = []
	row_count_dict: Dict[str, int] = {}
	for comp_index, compression in enumerate(comp_types):
		for index in range(13):
			with open('experiment/_compress_orc_profile_%d.txt' % ((comp_index + 1) * 13 + index)) as file:
				lines = file.readlines()
			assert(len(lines) == 1)
			line = lines[0]
			name = line.split()[0]

			blocks = name.split('.')
			assert(blocks[1] == 'orc')

			blocks = blocks[0].split('_')
			assert(blocks[0] == 'compress')
			assert(blocks[1] == compression)
			row_count_str = blocks[2]
			if len(x_values) == index:
				x_values.append(row_count_str)
				# x_values.append(str_to_row_count(row_count_str))
				row_count_dict[row_count_str] = index

	csv = pd.read_csv('result/compress.csv')
	record_dict: Dict[str, List[List[float]]] = {}
	for compression in ['none', 'zstd']:
		record_dict[compression] = []
		for index in range(13):
			record_dict[compression].append([])
	for index in range(len(csv)):
		file_name = csv.loc[index, 'file']
		if file_name.endswith('.pq'):
			continue
		assert(file_name.endswith('.orc'))
		blocks = file_name.split('.')[0].split('_')
		compression = blocks[1]
		row_count_str = blocks[2]
		if compression == 'snappy':
			continue
		assert(compression in {'none', 'zstd'})
		record_dict[compression][row_count_dict[row_count_str]].append(float(csv.loc[index, 'time_(s)']) * 1000)

	y_values['none_total'] = []
	y_values['zstd_total'] = []
	for compression in ['none', 'zstd']:
		for index in range(13):
			time_evaluations = record_dict[compression][index]
			assert(len(time_evaluations) == 8)
			y_values['%s_total' % compression].append(sum(time_evaluations) / len(time_evaluations))

	y_values['none_kernels'] = [y_values['none_decode'][index] + y_values['none_decomp'][index] for index in range(13)]
	y_values['zstd_kernels'] = [y_values['zstd_decode'][index] + y_values['zstd_decomp'][index] for index in range(13)]

	_, ax = plt.subplots(1, 1, figsize=(5, 3.3))
	bar_width = 0.15
	bar_0 = np.arange(0, 0.5 * 13, 0.5)
	bar_1 = bar_0 + bar_width

	ax.bar(bar_0, y_values['zstd_total'], width=bar_width, label='zstd_total',
		color=PURPLES[1])
	ax.bar(bar_0, y_values['zstd_kernels'], width=bar_width, label='zstd_decomp',
		color=ORANGES[3])
	ax.bar(bar_0, y_values['zstd_decode'], width=bar_width, label='zstd_decode',
		color=ORANGES[5])
	ax.bar(bar_1, y_values['none_total'], width=bar_width, label='none_total',
		color=PURPLES[2])
	ax.bar(bar_1, y_values['none_kernels'], width=bar_width, label='none_decomp',
		color=GREENS[3])
	ax.bar(bar_1, y_values['none_decode'], width=bar_width, label='none_decode',
		color=GREENS[5])

	ax.set_xticks(bar_0 + 0.5 * bar_width, x_values, fontsize=6)
	ax.set_axisbelow(True)
	ax.grid(axis='y', linewidth=0.35)

	ax.set_ylabel('Time (ms)', fontsize=16)
	ax.set_xlabel('Row Count', fontsize=16)
	ax.legend(fontsize=8, frameon=False, loc='upper left', bbox_to_anchor=(0.0, 1.02))

	plt.tight_layout()
	plt.savefig('figure_profile/compress_tradeoff.png', dpi=300)

if __name__ == '__main__':
	main()
