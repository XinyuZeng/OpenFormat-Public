import os
from save import add_setting

depths = [1, 2, 3, 4, 5, 6, 8, 10]

def main():
	for depth in depths:
		if not os.path.exists('pq/depth_shallow_%s.pq' % depth) or not os.path.exists('orc/depth_shallow_%s.orc' % depth):
			os.chdir('../..')
			assert(os.system('python3 python/nested/generate_file.py python/nested/experiments/depth_shallow/%s.json' % depth) == 0)
			os.chdir('python/cudf')
			assert(os.system('mv ../nested/experiments/depth_shallow/%d.parquet pq/depth_shallow_%d.pq' % (depth, depth)) == 0)
			assert(os.system('mv ../nested/experiments/depth_shallow/%d.orc orc/depth_shallow_%d.orc' % (depth, depth)) == 0)

	for depth in depths:
		add_setting({'depth': depth}, 'depth_shallow_%d' % depth)

if __name__ == '__main__':
	main()
