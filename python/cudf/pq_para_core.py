import pyarrow.parquet as pq

from save import save_pq_setting

default_row_group_size = 1024 ** 2
row_group_sizes = [
	1024, 2 * 1024, 4 * 1024, 8 * 1024, 16 * 1024,
	32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024,
	512 * 1024,
	2 * 1024 ** 2, 4 * 1024 ** 2, 8 * 1024 ** 2,
	16 * 1024 ** 2, 32 * 1024 ** 2
]
default_page_size = 1024 ** 2
page_sizes = [
	1024, 2 * 1024, 4 * 1024, 8 * 1024, 16 * 1024,
	32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024,
	512 * 1024,
	2 * 1024 ** 2, 4 * 1024 ** 2, 8 * 1024 ** 2,
	16 * 1024 ** 2, 32 * 1024 ** 2
]

def main():
	table = pq.read_table('pq/core_core_256K.pq')
	# Sweep row group size
	for row_group_size in row_group_sizes:
		pq.write_table(table, 'pq/pq_para_core_row_group_%d.pq' % row_group_size,
			compression='SNAPPY',
			use_dictionary=True,
			dictionary_pagesize_limit=1048576,
			row_group_size=row_group_size,
			version='2.6',
			data_page_version='2.0'
		)
		save_pq_setting(table, {
			'row_group_size': row_group_size,
			'page_size': default_page_size
		}, 'pq_para_core_row_group_%d' % row_group_size)
		
	# Sweep page size
	for page_size in page_sizes:
		pq.write_table(table, 'pq/pq_para_core_page_%d.pq' % page_size,
			compression='SNAPPY',
			use_dictionary=True,
			dictionary_pagesize_limit=1048576,
			row_group_size=default_row_group_size,
			data_page_size=page_size,
			version='2.6',
			data_page_version='2.0'
		)
		save_pq_setting(table, {
			'row_group_size': default_row_group_size,
			'page_size': page_size
		}, 'pq_para_core_page_%d' % page_size)
		
	pq.write_table(table, 'pq/pq_para_core_default.pq',
		compression='SNAPPY',
		use_dictionary=True,
		dictionary_pagesize_limit=1048576,
		row_group_size=default_row_group_size,
		version='2.6',
		data_page_version='2.0'
	)
	save_pq_setting(table, {
		'row_group_size': default_row_group_size,
		'page_size': default_page_size
	}, 'pq_para_core_default')

if __name__ == '__main__':
	main()
