import sys
from matplotlib import pyplot as plt
import matplotlib.ticker as ticker
import matplotlib
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42
import numpy as np
matplotlib.rcParams.update({'font.size': 14})

sys.path.append('.')
sys.path.append('python/plot')

from python.plot.stylelib import *
from python.plot.process_helper import *

depths = [2,4, 8, 17, 32, 47, 62]
fig_size = (4, 1.8)

def main():
	_, line_colors = select_color_idx(5)
	print(line_colors)
	
	file_name = 'python/nested/experiments/depth_size/stats.txt'
	with open(file_name, 'rt') as file:
		lines = file.readlines()
	
	lines = [[int(block) for block in line.split()] for line in lines]
	pq_size = [size[0] for size in lines]
	assert(len(pq_size) == len(depths))
	
	orc_size = [size[1] for size in lines]
	assert(len(orc_size) == len(depths))

	size_ratio = [pq_size[index] / orc_size[index] for index in range(len(depths))]

	pq_size = [size / (1024 ** 2) for size in pq_size]
	orc_size = [size / (1024 ** 2) for size in orc_size]
 
	file_name = 'python/nested/experiments/depth_size/scan_time_arrow.txt'
	with open(file_name, 'rt') as file:
		lines = file.readlines()
	
	lines = [[float(block) for block in line.split()] for line in lines]
	pq_time = [size[0] for size in lines]
	assert(len(pq_time) == len(depths))
	
	orc_time = [size[1] for size in lines]
	assert(len(orc_time) == len(depths))
	time_ratio = [pq_time[index] / orc_time[index] for index in range(len(depths))]

	pq_time = [time for time in pq_time]
	orc_time = [time for time in orc_time]
 
	file_name = 'python/nested/experiments/depth_size/scan_time.txt'
	with open(file_name, 'rt') as file:
		lines = file.readlines()
	
	lines = [[float(block) for block in line.split()] for line in lines]
	pq_time_raw = [size[0] for size in lines]
	assert(len(pq_time_raw) == len(depths))
	
	orc_time_raw = [size[1] for size in lines]
	assert(len(pq_time_raw) == len(depths))
	time_ratio = [pq_time_raw[index] / pq_time_raw[index] for index in range(len(depths))]
	pq_time_raw = [time / (1000 ** 2) for time in pq_time_raw]
	orc_time_raw = [time / (1000 ** 2) for time in orc_time_raw]

	fig, ax = plt.subplots(1, 1, figsize=fig_size)
	# ax_0 = ax[0]

	# ax_0.plot(
	# 	depths,
	# 	size_ratio,
	# 	color=line_colors['GREEN'],
	# 	marker='o',
	# 	label='Ratio',
	# 	ls=':',
	# 	ms=8
	# )
	# ax_0.set_xlabel('Depth')
	# ax_0.set_ylabel('Parquet Size / ORC Size')
	# ax_0.set_title('File Size Ratio')
	# ax_0.legend()

	ax.plot(
		depths,
		pq_size,
		color=line_colors[C_PQ],
		marker='v',
		label='Parquet',
		ls='-',
		ms=8
	)
	ax.plot(
		depths,
		orc_size,
		color=line_colors[C_ORC],
		marker='o',
		label='ORC',
		ls='-',
		ms=8
	)
	# for i, ratio in enumerate(size_ratio):
	# 	ax.text(depths[i], pq_size[i] + 5, "{:.0%}".format(ratio-1), ha='center', va='bottom')
	ax.set_xlabel('Max Depth')
	ax.set_xscale('log')
	# Set a logarithmic formatter for the x-axis ticks
	ax.xaxis.set_major_formatter(ticker.LogFormatter())
	# Set the x-axis ticks to the original data values
	ax.set_xticks(depths)
	# Set the x-axis tick labels to the original data values
	ax.set_xticklabels(depths)

	ax.set_axisbelow(True)
	ax.grid(axis='y', linewidth=0.35)
	ax.set_ylabel('File Size (MB)')
	legend = ax.legend(frameon=False, ncol=1, bbox_to_anchor=(0.4, 1.02), loc='upper center')
	export_legend(legend, "python/figures/nested_legend.pdf")
	# legend.remove()
	fig.savefig(f'python/figures/nested_size.pdf', bbox_inches = 'tight')
 
	fig, ax = plt.subplots(1, 1, figsize=fig_size)
	ax.plot(
		depths,
		pq_time,
		color=line_colors[C_PQ],
		marker='v',
		label='Parquet',
		ls='-',
		ms=8
	)
	ax.plot(
		depths,
		orc_time,
		color=line_colors[C_ORC],
		marker='o',
		label='ORC',
		ls='-',
		ms=8
	)
	# for i, ratio in enumerate(time_ratio):
	# 	ax.text(depths[i], orc_time[i] + 5, "{:.0%}".format(ratio-1), ha='center', va='bottom')
	ax.set_xlabel('Max Depth')
	ax.set_xscale('log')
	# Set a logarithmic formatter for the x-axis ticks
	ax.xaxis.set_major_formatter(ticker.LogFormatter())
	# Set the x-axis ticks to the original data values
	ax.set_xticks(depths)
	# Set the x-axis tick labels to the original data values
	ax.set_xticklabels(depths)
	ax.set_ylabel('Time (s)')
	ax.set_axisbelow(True)
	ax.grid(axis='y', linewidth=0.35)
	# ax.set_ylabel('Nested Info Overhead (ms)')
	# ax[1].legend()
	# plt.subplots_adjust(wspace=0.3)
	# fig.tight_layout()
	fig.savefig(f'python/figures/nested_time_arrow.pdf', bbox_inches = 'tight')
 
	fig, ax = plt.subplots(1, 1, figsize=fig_size)
	ax.plot(
		depths,
		np.array(pq_time_raw) / 1000,
		color=line_colors[C_PQ],
		marker='v',
		label='Parquet',
		ls='-',
		ms=8
	)
	ax.plot(
		depths,
		np.array(orc_time_raw) / 1000,
		color=line_colors[C_ORC],
		marker='o',
		label='ORC',
		ls='-',
		ms=8
	)
	# for i, ratio in enumerate(time_ratio):
	# 	ax.text(depths[i], orc_time[i] + 5, "{:.0%}".format(ratio-1), ha='center', va='bottom')
	ax.set_xlabel('Max Depth')
	ax.set_xscale('log')
	# Set a logarithmic formatter for the x-axis ticks
	ax.xaxis.set_major_formatter(ticker.LogFormatter())
	# Set the x-axis ticks to the original data values
	ax.set_xticks(depths)
	# Set the x-axis tick labels to the original data values
	ax.set_xticklabels(depths)
	# ax.set_ylabel('Time (ms)')
	ax.set_axisbelow(True)
	ax.grid(axis='y', linewidth=0.35)
	# ax[1].legend()
	# plt.subplots_adjust(wspace=0.3)
	# fig.tight_layout()
	fig.savefig(f'python/figures/nested_time.pdf', bbox_inches = 'tight')

if __name__ == '__main__':
	main()