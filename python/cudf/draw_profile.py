from typing import Dict, List, Tuple, Set
from matplotlib import pyplot as plt
import pandas as pd
import json

from analyze_profile import analyze_csv

y_labels = [
	'kernel_count',
	'unique_kernel_count',
	'total_duration_us',
	'longest_kernel_name',
	'longest_kernel_duration_us',
	'overall_Compute (SM) Throughput',
	'overall_Memory Throughput',
	'overall_Achieved Occupancy',
	'overall_Theoretical Occupancy'
]
color_dict = {'pq': 'red', 'orc': 'blue'}
ls_dict = {
	'snappy': '--', 'none': '-', 'zstd': ':'
}

def main():
	file_types = ['orc', 'pq']
	comp_types = ['snappy', 'none', 'zstd']
	stats_dict: Dict[Tuple[str, str], List[Dict]] = {}
	for file_type in file_types:
		for comp_index, compression in enumerate(comp_types):
			stats_list: List[Dict] = []
			for index in range(13):
				csv = pd.read_csv('ncu_csv/compress_%s_%d.csv' % (file_type, comp_index * 13 + index))
				stats_list.append(analyze_csv(csv, 'dict'))
			stats_dict[file_type, compression] = stats_list
		
	x_values = list(range(13))
	for y_label in y_labels:
		plt.ylabel(y_label)
		for file_type in file_types:
			for compression in comp_types:
				plt.plot(x_values, [stats[y_label] for stats in stats_dict[file_type, compression]],
	     			label='%s %s' % (file_type, compression),
					color=color_dict[file_type], ls=ls_dict[compression], marker='x')
		plt.legend()
		plt.savefig('figure_profile/compress_profile_%s.png' % y_label, dpi=300)
		plt.clf()

	y_label = 'occupancy_ratio'
	plt.ylabel(y_label)
	for file_type in file_types:
		for compression in comp_types:
			plt.plot(x_values, [stats['overall_Achieved Occupancy'] / stats['overall_Theoretical Occupancy'] for stats in stats_dict[file_type, compression]],
				label='%s %s' % (file_type, compression),
				color=color_dict[file_type], ls=ls_dict[compression], marker='x')
	plt.ylim(bottom=0, top=1)
	plt.grid(axis='y', linewidth=0.35)
	plt.legend()
	plt.savefig('figure_profile/compress_profile_%s.png' % y_label, dpi=300)
	plt.clf()

	output_dict: Dict[str, List[Dict]] = {}
	for file_type, compression in stats_dict:
		output_dict['%s_%s' % (file_type, compression)] = stats_dict[file_type, compression]
	with open('temp_compression_profile.json', 'wt') as file:
		json.dump(output_dict, file, indent=2)

if __name__ == '__main__':
	main()
