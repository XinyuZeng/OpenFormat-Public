# CAUTION: there is no header in the original core dataset generated
# To use these scripts, the .csv files must be generated with a header
# User may want to search ".csv" in benchmark/generator_v2/gen_workloads.py,
# and add headers (even a dummy) for the .csv files
# E.g., instead of header=False, use header=['col_%d' % index for index in range(32)]
# And also comment line 453, 454 in file gen_workload.py to avoid useless abortion

import os
from save import save_from_csv

core_row_counts_dict = {
	'int': [
		'4K', '8K', '12K', '16K',
		'32K', '64K', '128K', '256K', '512K',
		'1M', '2M', '4M', '8M', '16M', '32M'
	],
	'float': [
		'4K', '8K', '12K', '16K',
		'32K', '64K', '128K', '256K', '512K',
		'1M', '2M', '4M', '8M', '16M'
	],
	'string': [
		'4K', '8K', '12K', '16K',
		'32K', '64K', '128K', '256K', '512K',
		'1M', '2M', '4M', '8M'
	],
	'core': [
		'4K', '8K', '12K', '16K',
		'32K', '64K', '128K', '256K', '512K',
		'1M', '2M', '4M', '8M'
	]
}

def _get_row_count(row_count: str) -> int:
	if row_count.endswith('K'):
		return int(row_count[:-1]) * 1024
	elif row_count.endswith('M'):
		return int(row_count[:-1]) * 1024 ** 2
	else:
		assert(row_count.endswith('G'))
		return int(row_count[:-1]) * 1024 ** 3

def _generate(row_count: str, workload: str):
	assert(workload in {'int', 'float', 'string', 'core'})
	os.chdir('../../benchmark/generator_v2')
	assert(os.system('python3 gen_workloads.py %s %d 32 %s_%s' % (workload, _get_row_count(row_count), workload, row_count)) == 0)
	os.chdir('../../python/cudf')
	save_from_csv('../../benchmark/generator_v2/%s_%s/gen_data/%s_%s.csv' % (workload, row_count, workload, row_count), {}, 'core_%s_%s' % (workload, row_count))
	assert(os.system('rm -rf ../../benchmark/generator_v2/%s_%s' % (workload, row_count)) == 0)

def generate_core(workload: str):
	for row_count in core_row_counts_dict[workload]:
		_generate(row_count, workload)

if __name__ == '__main__':
	assert(False)
