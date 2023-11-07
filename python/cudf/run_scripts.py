from typing import List, Dict
import os
import subprocess
import threading
import time
import sys
from queue import Queue

def run_cmd(line_index: int, cmd: str, result_queue: Queue=None):
	beg = time.time()
	process = subprocess.Popen(
		cmd,
		shell=True,
		stdout=subprocess.PIPE,
		stderr=subprocess.PIPE
	)

	out, err = process.communicate()
	end = time.time()
	ret = (line_index, process.returncode, out.decode(), err.decode(), end - beg)
	if result_queue is not None:
		result_queue.put(ret)
	else:
		return ret

def run_script(script: str) -> Dict[str, str]:
	with open(script, 'rt') as file:
		lines: List[str] = [line[:-1] for line in file.readlines()]
	ret: Dict[str, str] = {}
	if len(lines) == 0:
		ret['warning'] = 'empty script'
	
	should_parallel = False
	if len(lines) != 0 and lines[0] == '# para':
		should_parallel = True
	ret['para'] = str(should_parallel)

	fail_count = 0
	if should_parallel:
		threads: List[threading.Thread] = []
		result_queue = Queue()
		for index, cmd in enumerate(lines):
			threads.append(threading.Thread(target=run_cmd, args=(index, cmd, result_queue)))
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()
		for index in range(len(threads)):
			line_index, return_code, out, err, run_time = result_queue.get()
			ret['time_%d' % line_index] = '%.3f sec' % run_time
			if return_code != 0:
				ret['FAIL_%d' % fail_count] = line_index
				ret['FAIL_%d_ret' % fail_count] = return_code
				ret['FAIL_%d_out' % fail_count] = out
				ret['FAIL_%d_err' % fail_count] = err
				fail_count += 1
	else:
		for index, cmd in enumerate(lines):
			line_index, return_code, out, err, run_time = run_cmd(index, cmd)
			ret['time_%d' % line_index] = '%.3f sec' % run_time
			if return_code != 0:
				ret['FAIL_%d' % fail_count] = line_index
				ret['FAIL_%d_ret' % fail_count] = return_code
				ret['FAIL_%d_out' % fail_count] = out
				ret['FAIL_%d_err' % fail_count] = err
				fail_count += 1
	
	ret['fail_count'] = fail_count
	return ret

def main():
	assert(len(os.listdir('log')) == 0)

	scripts_file = sys.argv[1]
	with open(scripts_file, 'rt') as file:
		scripts = [line.strip() for line in file.readlines()]
	
	summary_file = open('log/_summary.log', 'at')
	for script in scripts:
		output = run_script(script)
		with open('log/%s.log' % script, 'at') as file:
			for key in output:
				file.write('%s: %s\n' % (key, output[key]))

		prefix = ''
		if output['fail_count'] != 0:
			prefix = 'FAILED '
		log_line = '%s%s' % (prefix, script)
		summary_file.write(log_line)
		summary_file.write('\n')
		summary_file.flush()
	summary_file.close()

if __name__ == '__main__':
	main()
