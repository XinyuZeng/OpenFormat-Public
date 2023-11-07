import cupy
import pyarrow as pa
import sys

from save import save

row_counts = [
	'4K', '8K', '12K', '16K',
	'32K', '64K', '128K', '256K', '512K',
	'1M', '2M', '3M', '4M', '5M', '6M', '7M', '8M',
	'12M', '16M', '24M', '32M',
	'64M', '128M', '256M'
]

def get_repitition(row_count: str) -> int:
	if row_count.endswith('K'):
		return int(row_count[:-1]) // 4
	elif row_count.endswith('M'):
		return int(row_count[:-1]) * 1024 // 4
	else:
		assert(row_count.endswith('G'))
		return int(row_count[:-1]) * 1024 ** 2 // 4

def main():
	# 1K rows
	data = cupy.random.randint(-2 ** 63, 2 ** 63, 1024, dtype='int64').tolist()
	# Repeats for 4 times, i.e., 4K rows
	data = data * 4

	for row_count in row_counts:
		repitiion = get_repitition(row_count)
		table = pa.table([data * repitiion], names=['value'])
		save(table, {}, 'row_count_%s' % row_count)

if __name__ == '__main__':
	mode = sys.argv[1]
	if mode == 'name':
		for row_count in row_counts:
			print('row_count_%s' % row_count)
	else:
		assert(mode == 'generate')
		main()
