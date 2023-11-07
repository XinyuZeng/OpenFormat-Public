import sys
import os

def main():
	exp_name = sys.argv[1]
	exp_file = 'experiment/%s.txt' % exp_name

	to_import = False
	to_csv = False
	to_stats = False
	if len(sys.argv) > 2:
		if sys.argv[2] == 'import':
			to_import = True
		elif sys.argv[2] == 'csv':
			to_csv = True
		elif sys.argv[2] == 'stats':
			to_stats = True
	
	with open(exp_file, 'rt') as file:
		lines = file.readlines()
	
	for line in lines:
		block = line.split()[0]
		assert(not block.endswith('.pq'))
		assert(not block.endswith('.orc'))

	for index, line in enumerate(lines):
		name = '_%s_%%s_profile_%d' % (exp_name, index)
		for suffix in ['pq', 'orc']:
			if to_import:
				print('/usr/local/cuda/bin/ncu --import ncu/{0}.ncu-rep > ncu_report/{0}.txt'.format('%s_%s_%d' % (exp_name, suffix, index)))
			elif to_csv:
				print('/usr/local/cuda/bin/ncu --csv --import ncu/{0}.ncu-rep > ncu_csv/{0}.csv'.format('%s_%s_%d' % (exp_name, suffix, index)))
			elif to_stats:
				print('python3 analyze_profile.py {0} > ncu_stats/{0}.txt'.format('%s_%s_%d' % (exp_name, suffix, index)))
			else:
				full_name = name % suffix
				path = 'experiment/%s.txt' % full_name
				if not os.path.exists(path):
					with open(path, 'wt') as file:
						file.write('%s.%s cudf false\n' % (line.split()[0], suffix))
				print('/usr/local/cuda/bin/ncu -o ncu/%s_%s_%d python3 main_profile.py %s.txt' % (exp_name, suffix, index, full_name))

if __name__ == '__main__':
	main()
