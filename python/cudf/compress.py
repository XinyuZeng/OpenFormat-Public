import pyarrow.parquet as pq
import pyarrow.orc as orc

from save import save_pq_setting, save_orc_setting
from generate_core import core_row_counts_dict

row_counts = core_row_counts_dict['core']
compressions = [
	'none',
	'snappy',
	'zstd'
]

orc_compression_dict = {
	'none': 'uncompressed',
	'snappy': 'snappy',
	'zstd': 'zstd'
}

def main():
	for row_count in row_counts:
		table = pq.read_table('pq/core_core_%s.pq' % row_count)
		for compression in compressions:
			pq.write_table(table, 'pq/compress_%s_%s.pq' % (compression, row_count),
				compression=compression,
				use_dictionary=True,
				dictionary_pagesize_limit=1048576,
				row_group_size=1048576,
				version='2.6',
				data_page_version='2.0'
			)
			save_pq_setting(table, {
				'compression': compression
			}, 'compress_%s_%s' % (compression, row_count))

			orc.write_table(table, 'orc/compress_%s_%s.orc' % (compression, row_count),
				compression=orc_compression_dict[compression],
				compression_strategy='speed',
				dictionary_key_size_threshold=0.8
			)
			save_orc_setting(table, {
				'compression': compression
			}, 'compress_%s_%s' % (compression, row_count))

if __name__ == '__main__':
	main()
