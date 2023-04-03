import sys
import os

cpu_limits = [100, 200, 400, 800, 1600, 2400, 3200]

def main():
	experiment_name = sys.argv[1]
	for cpu_limit in cpu_limits:
		command = 'scripts/experiment.sh cpulimit -l %d python3 scripts/read_parquet.py %s' % (cpu_limit, experiment_name)
		returned_value = os.system(command)
		if returned_value:
			print('Error when running command:\n%s\nReturned value: %d' % (command, returned_value))
			sys.exit(returned_value)
		os.system('mv io_profile.log %s' % os.path.join('result', 'profiling', experiment_name, 'io_%d.log' % cpu_limit))
		os.system('mv cpu_profile.log %s' % os.path.join('result', 'profiling', experiment_name, 'cpu_%d.log' % cpu_limit))

if __name__ == '__main__':
	main()