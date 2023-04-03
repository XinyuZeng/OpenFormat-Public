import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.orc as po

import pathlib
import sys

sys.path.append('.')

from python.scripts.utils import *
from nested_data import *

size_names = ['', 'k', 'M', 'G', 'T', 'P']

# Use byte count to generate human readable file size
def _readable_size(size_byte: int):
	size_index = 0
	while size_byte >= 1024:
		size_index += 1
		size_byte /= 1024
	assert(size_index < len(size_names))
	return '%.2f%sB' % (size_byte, size_names[size_index])

def main():
	# Code from nested_exp.ipynb to use plain encoding for output files
	dir_path = pathlib.Path(os.path.abspath('')).resolve()
	HOME_DIR = str(dir_path).split('/OpenFormat')[0]
	PROJ_SRC_DIR = f'{HOME_DIR}/OpenFormat'
	sys.path.insert(1, f'{PROJ_SRC_DIR}')
	pq_config = enumerate_config(f'{PROJ_SRC_DIR}/python/experiments/pq_plain.json')
	pq_config = pq_config[0]
	orc_config = enumerate_config(f'{PROJ_SRC_DIR}/python/experiments/orc_plain.json')
	orc_config = orc_config[0]

	config = parse_config()

	# To change pq_config and orc_config if to use dictionary
	# By default dictionary won't be used
	if config['use_dict']:
		pq_config['use_dictionary'] = True
		orc_config['dictionary_key_size_threshold'] = 1
	
	print('Generating data...', end='\t', flush=True)
	begin = time.time()
	ret = generate_nested(config['row_count'], config['depth'], config)
	duration = time.time() - begin
	print('%.3fs' % duration)

	print('Transforming table...', end='\t', flush=True)
	begin = time.time()
	table = pa.table([ret], names=['col'])
	duration = time.time() - begin
	print('%.3fs' % duration)

	print('Writing parquet file...', end='\t', flush=True)
	begin = time.time()
	pq.write_table(table, '%s.parquet' % config['file_name'], **pq_config)
	duration = time.time() - begin
	print('%.3fs' % duration)

	# Orc file costs more time than parquet file
	# when depth is large
	print('Writing orc file...', end='\t', flush=True)
	begin = time.time()
	po.write_table(table, '%s.orc' % config['file_name'], **orc_config)
	duration = time.time() - begin
	print('%.3fs' % duration)

	print('Parquet size: %s' % _readable_size(os.path.getsize('%s.parquet' % config['file_name'])))
	print('Orc size: %s' % _readable_size(os.path.getsize('%s.orc' % config['file_name'])))

if __name__ == '__main__':
	main()
