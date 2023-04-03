import time
import sys
import os
import re
import getpass

user_name = getpass.getuser()
home_dir = '/home/%s' % user_name
file_re = re.compile('^lock_\d+_\d\d-\d\d-\d\d-\d\d:\d\d:\d\d$')

def get_time_tuple(lock_name):
	return (int(lock_name[-17:-15]), int(lock_name[-14:-12]), int(lock_name[-11:-9]), int(lock_name[-8:-6]), int(lock_name[-5:-3]), int(lock_name[-2:]))

def exists_previous_lock(locks, lock_name):
	time_tuple = get_time_tuple(lock_name)
	for lock in locks:
		if get_time_tuple(lock) < time_tuple:
			print('previous lock: %s\t current lock: %s' % (lock, lock_name))
			return True
	return False

def is_idle(lock_name):
	if not os.path.exists('%s/idle' % home_dir):
		print('No idle detected')
		return False
	locks = list(filter(file_re.search, os.listdir(home_dir)))
	if exists_previous_lock(locks, lock_name):
		print('Previous lock detected')
		return False
	os.system('mv %s/idle %s/busy' % (home_dir, home_dir))
	return True

def free(lock_name):
	os.system('mv %s/busy %s/idle' % (home_dir, home_dir))
	os.system('rm %s/%s' % (home_dir, lock_name))

def main():
	run_time = time.time()
	str_time = time.strftime('%y-%m-%d-%H:%M:%S', time.localtime(run_time))
	if not os.path.exists('stub.sh'):
		print('ERROR: No file named stub.sh.')
		sys.exit(1)
	os.system('chmod 744 stub.sh')
	os.system('mv stub.sh history/%s.sh' % str_time)
	lock_name = 'lock_%d_%s' % (os.getpid(), str_time)
	os.system('touch %s/%s' % (home_dir, lock_name))
	time.sleep(1)
	while not is_idle(lock_name):
		time.sleep(300)
	os.system('history/%s.sh' % str_time)
	free(lock_name)

if __name__ == '__main__':
	main()