from typing import Dict, Tuple, List, Set
import pandas as pd
import sys

metric_name_list = [
	'DRAM Frequency', 'SM Frequency', 'Elapsed Cycles',
	'Memory Throughput', 'DRAM Throughput', 'Duration',
	'L1/TEX Cache Throughput', 'L2 Cache Throughput',
	'SM Active Cycles', 'Compute (SM) Throughput', 'SpeedOfLight',
	'Block Size', 'Function Cache Configuration', 'Grid Size',
	'Registers Per Thread', 'Shared Memory Configuration Size',
	'Driver Shared Memory Per Block', 'Dynamic Shared Memory Per Block',
	'Static Shared Memory Per Block', 'Threads', 'Waves Per SM', 'LaunchStats',
	'Block Limit SM', 'Block Limit Registers', 'Block Limit Shared Mem',
	'Block Limit Warps', 'Theoretical Active Warps per SM',
	'Theoretical Occupancy', 'Achieved Occupancy',
	'Achieved Active Warps Per SM', 'Occupancy'
]
metric_name_set = set(metric_name_list)
metric_name_dict: Dict[str, int] = {}
for index, metric_name in enumerate(metric_name_list):
	metric_name_dict[metric_name] = index
metric_count = len(metric_name_list)

class KernelStats:
	def __init__(self, csv: pd.DataFrame, start: int) -> None:
		self.stats: Dict[str, int] = {}
		self.conclusion: Dict[str, int] = {}
		self.range: Tuple[int, int] = (None, None)
		id: int = None
		index = start
		while True:
			if id is None:
				id = csv.loc[start, 'ID']
			if index == len(csv) or csv.loc[index, 'ID'] != id:
				self.range = (start, index)
				break
			if isinstance(csv.loc[index, "Metric Name"], str):
				self.stats[csv.loc[index, "Metric Name"]] = index
			else:
				self.conclusion[csv.loc[index, 'Metric Unit']] = index
			index += 1
		assert(len(self.stats) == 28)

def get_usecond_duration(csv: pd.DataFrame, csv_index: int) -> float:
	duration = float(csv.loc[csv_index, 'Metric Value'])
	if csv.loc[csv_index, 'Metric Unit'] == 'msecond':
		duration *= 1000
	else:
		assert(csv.loc[csv_index, 'Metric Unit'] == 'usecond')
	return duration

def usecond_to_str(duration: float) -> str:
	if duration >= 1000:
		ret = '%.2f ms' % (duration / 1000)
	else:
		ret = '%.2f us' % duration
	return ret

def analyze_csv(csv: pd.DataFrame, mode: str) -> int or Dict:
	all_stats: List[KernelStats] = []
	next = 0
	while next != len(csv):
		all_stats.append(KernelStats(csv, next))
		next = all_stats[-1].range[1]

	kernel_dict: Dict[str, float] = {}
	kernel_count: Dict[str, int] = {}
	kernel_set: Set[str] = set()
	weighted_stats: Dict[str, Dict[str, Tuple[float, float]]] = {}
	weighted_keys = [
		'Compute (SM) Throughput',
		'Memory Throughput',
		'Achieved Occupancy',
		'Theoretical Occupancy'
	]
	for stats in all_stats:
		csv_index = stats.stats['Duration']
		kernel_name = csv.loc[csv_index, 'Kernel Name']
		if kernel_name not in kernel_set:
			kernel_set.add(kernel_name)
			kernel_dict[kernel_name] = 0.0
			kernel_count[kernel_name] = 0
			weighted_stats[kernel_name] = {}
			for key in weighted_keys:
				weighted_stats[kernel_name][key] = (0.0, 0.0)
		duration = get_usecond_duration(csv, csv_index)
		kernel_dict[kernel_name] += duration
		kernel_count[kernel_name] += 1
		for key in weighted_keys:
			csv_index = stats.stats[key]
			assert(csv.loc[csv_index, 'Metric Unit'] == '%')
			utilized_duration = duration * float(csv.loc[csv_index, 'Metric Value']) / 100
			prev_tuple = weighted_stats[kernel_name][key]
			weighted_stats[kernel_name][key] = (prev_tuple[0] + utilized_duration, prev_tuple[1] + duration)

	overall_stats: Dict[str, float] = {}
	for key in weighted_keys:
		utilized_duration = 0.0
		total_duration = 0.0
		for kernel_name in weighted_stats:
			utilized_duration += weighted_stats[kernel_name][key][0]
			total_duration += weighted_stats[kernel_name][key][1]
		overall_stats[key] = utilized_duration / total_duration

	kernel_list: List[Tuple[str, float]] = []
	for kernel_name in kernel_dict:
		kernel_list.append((kernel_name, kernel_dict[kernel_name]))
	kernel_list.sort(key=lambda x: x[1], reverse=True)

	duration_sum = 0.0
	for kernel_name, duration in kernel_list:
		duration_sum += duration
		duration_str = usecond_to_str(duration)

	if mode == 'text':
		print('Kernel count: %d' % len(all_stats))
		for stats in all_stats:
			csv_index = stats.stats['Duration']
			print('%s %s %s' % (
				csv.loc[csv_index, 'Metric Value'],
				csv.loc[csv_index, 'Metric Unit'],
				csv.loc[csv_index, 'Kernel Name']
			))

		print()
		print('Unique kernels: %d' % len(kernel_set))
		for kernel_name, duration in kernel_list:
			duration_str = usecond_to_str(duration)
			print('%s (%d) %s' % (duration_str, kernel_count[kernel_name], kernel_name))
			
		print()
		print('Total: %s' % usecond_to_str(duration_sum))
		for kernel_name, duration in kernel_list:
			print('%.2f%% (%d) %s' % (duration / duration_sum * 100, kernel_count[kernel_name], kernel_name))

		for key in weighted_keys:
			print()
			print('Overall %s: %.2f%%' % (key, overall_stats[key] * 100))
			for kernel_name, duration in kernel_list:
				stat_value = weighted_stats[kernel_name][key][0] / weighted_stats[kernel_name][key][1]
				print('%.2f%% %.2f%% (%d) %s' % (duration / duration_sum * 100, stat_value * 100, kernel_count[kernel_name], kernel_name))
	elif mode == 'dict':
		stats_dict = {}
		stats_dict['kernel_count'] = len(all_stats)
		stats_dict['unique_kernel_count'] = len(kernel_set)
		stats_dict['total_duration_us'] = duration_sum
		stats_dict['longest_kernel_name'] = kernel_list[0][0]
		stats_dict['longest_kernel_duration_us'] = kernel_list[0][1]
		stats_dict['kernel_list'] = [(kernel_name, duration, kernel_count[kernel_name]) for kernel_name, duration in kernel_list]
		for key in overall_stats:
			stats_dict['overall_%s' % key] = overall_stats[key]
		return stats_dict
	else:
		print('ERROR: unknown mode: %s' % mode)
		assert(False)
	return 0

if __name__ == '__main__':
	if len(sys.argv) > 1:
		csv = pd.read_csv('ncu_csv/%s.csv' % sys.argv[1])
	else:
		print('CAUTION: Reading from ncu_csv/compress_pq_38.csv')
		csv = pd.read_csv('ncu_csv/compress_pq_38.csv')
	
	sys.exit(analyze_csv(csv, 'text'))
