from typing import Tuple
import numpy as np
import pyarrow as pa
from save import save_customize

# See python/experiments/pq_encoding.json
pq_dict = {
	'compression': 'NONE',
	'use_dictionary': True,
	'dictionary_pagesize_limit': 2147483647,
	'version': '2.6',
	'data_page_version': '2.0'
}
# Mostly from python/experiments/orc_encoding.json, but use default stripe size 
orc_dict = {
	'compression': 'uncompressed',
	'compression_strategy': 'speed',
	'dictionary_key_size_threshold': 1
}

tuple_count = 8 * 1024 ** 2
pool_size = 4 * 1024 ** 2
string_length = 8
probs = [0.0, 0.002, 0.01, 0.02, 0.04, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
alphabet = [
	'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
	'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p',
	'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
]

def _index_to_str(index: int) -> str:
	characters = []
	for index in range(string_length):
		characters.append(alphabet[index % len(alphabet)])
		index //= len(alphabet)
	assert(index == 0)
	return ''.join(characters)

def _generate_random_string() -> str:
	return _index_to_str(np.random.randint(0, pool_size))

def generate_data(prob: float) -> Tuple[pa.Table, pa.Table]:
	data = []
	control_data = []
	for index in range(tuple_count):
		value = _generate_random_string()
		data.append(value)
		control_data.append(value)
		if np.random.rand() < prob:
			for index in range(7):
				data.append(value)
				control_data.append(_generate_random_string())
	return pa.table([data], names=['value']), pa.table([control_data], names=['value'])

def main():
	for prob in probs:
		print('rle_string_%.3f' % prob)
		print('rle_string_control_%.3f' % prob)
		data, control_data = generate_data(prob)
		save_customize(data, {'prob': prob, 'control': False}, 'rle_string_%.3f' % prob, pq_dict, orc_dict)
		save_customize(control_data, {'prob': prob, 'control': True}, 'rle_string_control_%.3f' % prob, pq_dict, orc_dict)

if __name__ == '__main__':
	main()
