import pyarrow.orc as orc

from save import save_orc_setting

default_stripe_size = 64 * 1024 ** 2
stripe_sizes_M = [4, 8, 16, 32, 96, 128, 256, 512, 1024]
default_row_index_stride = 10000
row_index_strides = [500, 1000, 2500, 5000, 15000, 20000, 40000, 100000, 200000, 400000, 1000000]

def main():
	table = orc.read_table('orc/core_int_2M.orc')
	# Sweep stripe size
	for stripe_size in stripe_sizes_M:
		orc.write_table(table, 'orc/orc_para_stripe_%dM.orc' % stripe_size,
			compression='SNAPPY',
			compression_strategy='speed',
			dictionary_key_size_threshold=0.8,
			stripe_size=stripe_size * 1024 ** 2,
			row_index_stride=default_row_index_stride
		)
		save_orc_setting(table, {
			'stripe_size': stripe_size * 1024 ** 2,
			'row_index_stride': default_row_index_stride
		}, 'orc_para_stripe_%dM' % stripe_size)
		
	# Sweep row index stride
	for row_index_stride in row_index_strides:
		orc.write_table(table, 'orc/orc_para_row_index_%d.orc' % row_index_stride,
			compression='SNAPPY',
			compression_strategy='speed',
			dictionary_key_size_threshold=0.8,
			stripe_size=default_stripe_size,
			row_index_stride=row_index_stride
		)
		save_orc_setting(table, {
			'stripe_size': default_stripe_size,
			'row_index_stride': row_index_stride
		}, 'orc_para_row_index_%d' % row_index_stride)
		
	# Using default setting for both
	orc.write_table(table, 'orc/orc_para_default.orc',
		compression='SNAPPY',
		compression_strategy='speed',
		dictionary_key_size_threshold=0.8,
		stripe_size=default_stripe_size,
		row_index_stride=default_row_index_stride
	)
	save_orc_setting(table, {
		'stripe_size': default_stripe_size,
		'row_index_stride': default_row_index_stride
	}, 'orc_para_default')

if __name__ == '__main__':
	main()
