from typing import Tuple
import numpy as np
import pyarrow as pa
import sys
from save import save

tuple_count = 8 * 1024 ** 2
probs = [0.0, 0.002, 0.01, 0.02, 0.04, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]

def generate_data(prob: float) -> Tuple[pa.Table, pa.Table]:
	data = []
	control_data = []
	for index in range(tuple_count):
		value = np.random.randint(-2 ** 63, 2 ** 63, dtype=np.int64)
		data.append(value)
		control_data.append(value)
		if np.random.rand() < prob:
			for index in range(7):
				data.append(value)
				control_data.append(np.random.randint(-2 ** 63, 2 ** 63, dtype=np.int64))
	return pa.table([data], names=['value']), pa.table([control_data], names=['value'])

def main():
	for prob in probs:
		print('rle_prob_%.3f' % prob)
		print('rle_prob_control_%.3f' % prob)
		data, control_data = generate_data(prob)
		save(data, {'prob': prob, 'control': False}, 'rle_prob_%.3f' % prob)
		save(control_data, {'prob': prob, 'control': True}, 'rle_prob_control_%.3f' % prob)

if __name__ == '__main__':
	mode = sys.argv[1]
	if mode == 'name':
		for prob in probs:
			print('rle_prob_%.3f' % prob)
			print('rle_prob_control_%.3f' % prob)
	else:
		assert(mode == 'generate')
		main()
