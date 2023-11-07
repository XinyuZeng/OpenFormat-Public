'''
Instead of running this script directly,
it's more suggested to use:
./run.sh <file_name>
'''

from typing import Tuple, List
import cudf
import rmm
import cupy
import pandas as pd
import sys
import os
import nvtx
import time
import pyarrow.parquet as pq
import pyarrow.orc as orc

def clear_cache():
	assert(os.system("sync; sudo /sbin/sysctl vm.drop_caches=3 > /dev/null") == 0)

def warm_up():
	warm_up_data = cudf.DataFrame({'a': cupy.random.randint(-1000, 1000, 1000000, dtype='int64')})
	warm_up_data.to_parquet('pq/warm_up.pq', compression='ZSTD')
	warm_up_data.to_orc('orc/warm_up.orc', compression='ZSTD')
	warm_up_data = cudf.read_parquet('pq/warm_up.pq')
	warm_up_data = cudf.read_orc('orc/warm_up.orc')

def run(file: Tuple[str, str, str], result: pd.DataFrame):
	file_names: List[str] = []
	if file[0].endswith('pq') or file[0].endswith('orc'):
		file_names.append(file[0])
	else:
		file_names.append('%s.pq' % file[0])
		file_names.append('%s.orc' % file[0])
	
	if file[1] == 'all':
		libs = ['cudf', 'pandas', 'pyarrow']
	elif file[1] == 'default':
		libs = ['cudf', 'pyarrow']
	else:
		libs = [file[1]]
	
	for file_name in file_names:
		for lib in libs:
			if file[2] == 'true':
				clear_cache()
			else:
				assert(file[2] == 'false')
				if file_name.endswith('pq'):
					run_parquet(file_name, lib, result, True)
				else:
					assert(file_name.endswith('orc'))
					run_orc(file_name, lib, result, True)
			if file_name.endswith('pq'):
				run_parquet(file_name, lib, result)
			else:
				assert(file_name.endswith('orc'))
				run_orc(file_name, lib, result)
	
def run_parquet(file_name: str, lib: str, result, is_warm_up: bool=False):
	file_path = 'pq/%s' % file_name
	result_index = len(result)
	try:
		if lib == 'cudf':
			with nvtx.annotate(file_path, color='purple'):
				beg = time.time()
				table = cudf.read_parquet(file_path)
				end = time.time()
		elif lib == 'pandas':
			with nvtx.annotate(file_path, color='yellow'):
				beg = time.time()
				table = pd.read_parquet(file_path)
				end = time.time()
		else:
			assert(lib == 'pyarrow')
			with nvtx.annotate(file_path, color='yellow'):
				beg = time.time()
				table = pq.read_table(file_path)
				end = time.time()
		if not is_warm_up:
			result.at[result_index, 'file'] = file_name
			result.at[result_index, 'lib'] = lib
			result.at[result_index, 'time_(s)'] = end - beg
	except Exception as err:
		print('On file: %s' % file_name)
		print('ERROR: %s' % err)
		if not is_warm_up:
			result.at[result_index, 'error'] = str(err)[:100]

def run_orc(file_name: str, lib: str, result, is_warm_up: bool=False):
	file_path = 'orc/%s' % file_name
	result_index = len(result)
	try:
		if lib == 'cudf':
			with nvtx.annotate(file_path, color='purple'):
				beg = time.time()
				table = cudf.read_orc(file_path)
				end = time.time()
		elif lib == 'pandas':
			with nvtx.annotate(file_path, color='yellow'):
				beg = time.time()
				table = pd.read_orc(file_path)
				end = time.time()
		else:
			assert(lib == 'pyarrow')
			with nvtx.annotate(file_path, color='yellow'):
				beg = time.time()
				table = orc.read_table(file_path)
				end = time.time()
		if not is_warm_up:
			result.at[result_index, 'file'] = file_name
			result.at[result_index, 'lib'] = lib
			result.at[result_index, 'time_(s)'] = end - beg
	except Exception as err:
		print('On file: %s' % file_name)
		print('ERROR: %s' % err)
		if not is_warm_up:
			result.at[result_index, 'error'] = str(err)[:100]

def main():
	setting_file = sys.argv[1]
	assert(setting_file.endswith('.txt'))
	assert(len(setting_file.split('/')) == 1)
	with open('experiment/%s' % setting_file, 'rt') as file:
		files = [tuple(line.strip().split()) for line in file.readlines()]

	rmm.reinitialize(pool_allocator=True, initial_pool_size=12 * 10 ** 9)
	warm_up()

	result = pd.DataFrame()
	for index in range(8):
		for file in files:
			run(file, result)

	result.to_csv('result/%s.csv' % setting_file[:-4])

if __name__ == '__main__':
	main()
