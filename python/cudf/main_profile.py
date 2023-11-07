from typing import Tuple, List
import cudf
import rmm
import cupy
import sys
import os
import pyarrow.parquet as pq
import pyarrow.orc as orc

def warm_up():
	warm_up_data = cudf.DataFrame({'a': cupy.random.randint(-1000, 1000, 1000000, dtype='int64')})
	warm_up_data.to_parquet('pq/warm_up.pq', compression='ZSTD')
	warm_up_data.to_orc('orc/warm_up.orc', compression='ZSTD')
	warm_up_data = cudf.read_parquet('pq/warm_up.pq')
	warm_up_data = cudf.read_orc('orc/warm_up.orc')

def run(file: Tuple[str, str, str]):
	file_names: List[str] = []
	if file[0].endswith('pq') or file[0].endswith('orc'):
		file_names.append(file[0])
	else:
		file_names.append('%s.pq' % file[0])
		file_names.append('%s.orc' % file[0])
	
	for file_name in file_names:
		if file_name.endswith('pq'):
			run_parquet(file_name)
		else:
			assert(file_name.endswith('orc'))
			run_orc(file_name)
	
def run_parquet(file_name: str):
	file_path = 'pq/%s' % file_name
	try:
		pyarrow_table = pq.read_table(file_path)
		cudf_table = cudf.read_parquet(file_path)
	except Exception as err:
		print('On file: %s' % file_name)
		print('ERROR: %s' % err)

def run_orc(file_name: str):
	file_path = 'orc/%s' % file_name
	try:
		pyarrow_table = orc.read_table(file_path)
		cudf_table = cudf.read_orc(file_path)
	except Exception as err:
		print('On file: %s' % file_name)
		print('ERROR: %s' % err)

def main():
	setting_file = sys.argv[1]
	assert(setting_file.endswith('.txt'))
	assert(len(setting_file.split('/')) == 1)
	with open('experiment/%s' % setting_file, 'rt') as file:
		files = [tuple(line.strip().split()) for line in file.readlines()]

	rmm.reinitialize(pool_allocator=True, initial_pool_size=12 * 10 ** 9)
	# warm_up()

	for file in files:
		run(file)

if __name__ == '__main__':
	main()
