import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.orc as po
import pyarrow.compute as pc

import time

from nested_data import *

key_words = ['parquet', 'orc']

def aggregate_parquet_column(parquet):
	sum = 0.0
	for index in range(len(parquet)):
		if parquet[0][index].is_valid:
			sum += parquet[0][index].as_py()
	return sum

def aggregate_orc_column(orc, depth: int):
	sum = 0.0
	for index in range(len(orc)):
		node = orc[0][index]
		is_valid = True
		for depth_index in range(depth - 1):
			node = node[0]
			if not node.is_valid:
				is_valid = False
				break
		if is_valid:
			sum += node[0].as_py()
	return sum

def read_table(file_name: str, file_format: str, full_depth: int):
	assert(file_format in key_words)
	if file_format == 'parquet':
		module = pq
	else:
		assert(file_format == 'orc')
		module = po
		
	print('Reading %s table...' % file_format, end='\t', flush=True, file=sys.stderr)
	begin = time.time()
	ret = module.read_table('%s.%s' % (file_name, file_format), columns=['col' + '.next' * (full_depth - 1) + '.level_%d' % (full_depth - 1)])
	duration = time.time() - begin
	print('%.3fs' % duration, file=sys.stderr)

	return ret

def main():
	config = parse_config()
	
	pq_table = read_table(config['file_name'], 'parquet', config['depth'])
	orc_table = read_table(config['file_name'], 'orc', config['depth'])
	
	print('Aggregating Parquet column in Python...', end='\t', flush=True, file=sys.stderr)
	begin = time.time()
	pq_sum = aggregate_parquet_column(pq_table)
	duration = time.time() - begin
	print('', file=sys.stderr)
	print('%.6f' % duration, end=' ')
	
	print('Aggregating Parquet column using pyarrow.compute...', end='\t', flush=True, file=sys.stderr)
	begin = time.time()
	pc_sum = pc.sum(pq_table[0])
	duration = time.time() - begin
	print('', file=sys.stderr)
	print('%.6f' % duration, end=' ')
	
	print('Aggregating Orc column in Python...', end='\t', flush=True, file=sys.stderr)
	begin = time.time()
	orc_sum = aggregate_orc_column(orc_table, config['depth'])
	duration = time.time() - begin
	print('', file=sys.stderr)
	print('%.6f' % duration, end='\n')

	sum_0 = pq_sum
	sum_1 = pc_sum.as_py()
	sum_2 = orc_sum
	diff = max(abs(sum_0 - sum_1), abs(sum_1 - sum_2), abs(sum_2 - sum_0))
	card = max(abs(sum_0), abs(sum_1), abs(sum_2))
	if card * 1e-7 < diff:
		print('WARNING: checksum doesn\'t match!', file=sys.stderr)

	# table_identical = pq_table == orc_table
	# print('Identity check:', file=sys.stderr)
	# print('\tparquet == orc: %s' % table_identical, file=sys.stderr)

	# if table_identical:
	# 	sys.exit(0)
	# else:
	# 	print('ERROR: identity check or checksum failed', file=sys.stderr)
	# 	sys.exit(1)

if __name__ == '__main__':
	main()