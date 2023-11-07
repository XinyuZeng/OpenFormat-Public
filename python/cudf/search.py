from typing import Tuple, List, Dict, Set
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq
import pyarrow.orc as orc
import rmm
import os

from main import warm_up
from save import save_file_above_default, save_pq_partial_setting, save_orc_partial_setting
from measure_time import measure
from generate_core import core_row_counts_dict

row_counts = core_row_counts_dict['core']

para_dict = {
	'pq': (
		('row_group_size', [
			1024, 2 * 1024, 4 * 1024, 8 * 1024, 16 * 1024,
			32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024,
			512 * 1024, 1024 ** 2,
			2 * 1024 ** 2, 4 * 1024 ** 2, 8 * 1024 ** 2,
			16 * 1024 ** 2, 32 * 1024 ** 2
		], 10),
		('data_page_size', [
			1024, 2 * 1024, 4 * 1024, 8 * 1024, 16 * 1024,
			32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024,
			512 * 1024, 1024 ** 2,
			2 * 1024 ** 2, 4 * 1024 ** 2, 8 * 1024 ** 2,
			16 * 1024 ** 2, 32 * 1024 ** 2
		], 10)
	),
	'orc': (
		('stripe_size', [
			2 * 1024, 4 * 1024, 8 * 1024, 16 * 1024,
			32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024,
			512 * 1024, 1024 ** 2,
			2 * 1024 ** 2, 4 * 1024 ** 2, 8 * 1024 ** 2,
			16 * 1024 ** 2, 32 * 1024 ** 2,
			64 * 1024 ** 2
		], 15),
		('row_index_stride', [
			128, 256, 512,
			1024, 2 * 1024, 4 * 1024, 8 * 1024, 16 * 1024,
			32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024,
			512 * 1024, 1024 ** 2,
			2 * 1024 ** 2, 4 * 1024 ** 2
		], 6)
	)
}

libs = ['cudf', 'pyarrow']
file_types = ['pq', 'orc']

def get_file_name(lib: str, row_count: str) -> str:
	return 'search_%s_%s' % (lib, row_count)

def _generate_list_from_hint(hint: int, length: int) -> List[int]:
	# ret = []
	# for index in range(hint - 2, hint + 3):
	# 	if index >= 0 and index < length:
	# 		ret.append(index)
	# return ret
	return list(range(length))

def _get_relative_error(records: List[float]) -> float:
	average = sum(records) / len(records)
	square_distance = 0.0
	for record in records:
		square_distance += (record - average) ** 2
	relative_error = (square_distance / len(records) / (len(records) - 1)) ** 0.5 / average
	return relative_error

def _get_min_error(records: List[float], error_thres) -> List[float] or None:
	new_records = records.copy()
	while len(new_records) > len(records) / 2:
		new_records.remove(max(new_records))
		if _get_relative_error(new_records) < error_thres:
			return new_records
	return None

def _measure(data: pa.Table, lib: str, file_type: str, args: Dict, name: str):
	file_name = save_file_above_default(data, file_type, name, args)

	error_thres = 0.03
	error_diff = 0.01
	trial_index = 0
	while True:
		records: List[float] = []
		for index in range(3):
			records.append(measure(lib, file_name))
		while len(records) != 12 and _get_relative_error(records) >= error_thres:
			records.append(measure(lib, file_name))
		if _get_relative_error(records) < error_thres:
			break
		else:
			records = _get_min_error(records, error_thres)
			if records is not None:
				break
			trial_index += 1
			if trial_index == 3:
				trial_index = 0
				error_thres += error_diff
				error_diff += 0.01
				print('%s: error threshold becomes %.2f' % (name, error_thres), flush=True)
	os.remove(file_name)
	return sum(records) / len(records)

def find_optimal(lib: str, file_type: str, row_count: str, hint_0: int, hint_1: int) -> Tuple[int, int, List[float]]:
	if file_type == 'pq':
		data = pq.read_table('pq/core_core_%s.pq' % row_count)
	else:
		assert(file_type == 'orc')
		data = orc.read_table('orc/core_core_%s.orc' % row_count)
	
	hint_name_0 = para_dict[file_type][0][0]
	hint_list_0 = para_dict[file_type][0][1]
	hint_name_1 = para_dict[file_type][1][0]
	hint_list_1 = para_dict[file_type][1][1]
	
	measure_dict: Dict[Tuple[int, int], float] = {}
	measure_set: Set[Tuple[int, int]] = set()

	new_0 = True
	new_1 = True

	if hint_0 is None:
		opt_0 = para_dict[file_type][0][2]
		list_0 = list(range(len(hint_list_0)))
	else:
		opt_0 = hint_0
		list_0 = _generate_list_from_hint(opt_0, len(hint_list_0))
	if hint_1 is None:
		opt_1 = para_dict[file_type][1][2]
		list_1 = list(range(len(hint_list_1)))
	else:
		opt_1 = hint_1
		list_1 = _generate_list_from_hint(opt_1, len(hint_list_1))

	while new_0 or new_1:
		if new_1:
			results = []
			for value_0 in list_0:
				if (value_0, opt_1) not in measure_set:
					measure_set.add(((value_0, opt_1)))
					args = {
						hint_name_0: hint_list_0[value_0],
						hint_name_1: hint_list_1[opt_1]
					}
					measure_dict[(value_0, opt_1)] = _measure(data, lib, file_type, args, '_%s_%s_%d_%d' % (lib, row_count, value_0, opt_1))
				results.append(measure_dict[(value_0, opt_1)])
			cur_result = measure_dict[(opt_0, opt_1)]
			best_time = min(results)

			if opt_0 == hint_0:
				thres = 0.1
			else:
				thres = 0.05
			if (cur_result - best_time) / best_time > thres:
				opt_0 = list_0[results.index(best_time)]
				new_0 = True
			list_1 = _generate_list_from_hint(opt_1, len(hint_list_1))
			new_1 = False
		else:
			assert(new_0)
			results = []
			for value_1 in list_1:
				if (opt_0, value_1) not in measure_set:
					measure_set.add(((opt_0, value_1)))
					args = {
						hint_name_0: hint_list_0[opt_0],
						hint_name_1: hint_list_1[value_1]
					}
					measure_dict[(opt_0, value_1)] = _measure(data, lib, file_type, args, '_%s_%s_%d_%d' % (lib, row_count, opt_0, value_1))
				results.append(measure_dict[(opt_0, value_1)])
			cur_result = measure_dict[(opt_0, opt_1)]
			best_time = min(results)

			if opt_1 == hint_1:
				thres = 0.1
			else:
				thres = 0.05
			if (cur_result - best_time) / best_time > thres:
				opt_1 = list_1[results.index(best_time)]
				new_1 = True
			list_0 = _generate_list_from_hint(opt_0, len(hint_list_0))
			new_0 = False

	opt_args = {
		hint_name_0: hint_list_0[opt_0],
		hint_name_1: hint_list_1[opt_1]
	}
	raw_file_name = get_file_name(lib, row_count)
	file_path = save_file_above_default(data, file_type, raw_file_name, opt_args)
	if file_type == 'pq':
		save_pq_partial_setting(data, opt_args, raw_file_name)
	else:
		assert(file_type == 'orc')
		save_orc_partial_setting(data, opt_args, raw_file_name)

	records = []
	for index in range(8):
		records.append(measure(lib, file_path))
	return opt_0, opt_1, records

def main():
	rmm.reinitialize(pool_allocator=True, initial_pool_size=12 * 10 ** 9)
	warm_up()

	result = pd.DataFrame()
	for lib in libs:
		for file_type in file_types:
			opt_0 = None
			opt_1 = None
			for row_count in row_counts:
				opt_0, opt_1, records = find_optimal(lib, file_type, row_count, opt_0, opt_1)

				assert(len(records) == 8)
				file_name = '%s.%s' % (get_file_name(lib, row_count), file_type)
				result_begin = len(result)
				for result_index in range(result_begin, result_begin + 8):
					result.at[result_index, 'file'] = file_name
					result.at[result_index, 'lib'] = lib
					result.at[result_index, 'time_(s)'] = records[result_index - result_begin]
				print('%s %s %s finished' % (lib, file_type, row_count), flush=True)
	result.to_csv('result/search.csv')

if __name__ == '__main__':
	main()
