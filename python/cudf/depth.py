import os
from save import add_setting

depths = [2, 4, 8, 17, 32, 47, 62]

def main():
	for depth in depths:
		if not os.path.exists('pq/depth_%s.pq' % depth) or not os.path.exists('orc/depth_%s.orc' % depth):
			os.chdir('../..')
			assert(os.system('python3 python/nested/generate_file.py python/nested/experiments/depth_size/%s.json' % depth) == 0)
			os.chdir('python/cudf')
			assert(os.system('mv ../nested/experiments/depth_size/%d.parquet pq/depth_%d.pq' % (depth, depth)) == 0)
			assert(os.system('mv ../nested/experiments/depth_size/%d.orc orc/depth_%d.orc' % (depth, depth)) == 0)

	for depth in depths:
		add_setting({'depth': depth}, 'depth_%d' % depth)

if __name__ == '__main__':
	main()
