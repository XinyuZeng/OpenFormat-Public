import numpy as np
import math
import sys
import json

default_config = {
	# Use distinct float to utilize dictionary encoding
	# Used in nested_data._generate_node()
	'distinct_float': False,

	# Define the depth of the schema
	# 62 is the biggest value s.t. the Parquet file
	# can be read from disk without error
	'depth': 62,

	# Number of rows in the output table
	# Default settings make the output file about
	# tens of megabytes
	'row_count': 1024 * 64,

	# Probability of a node to have two children
	# Can't be too large in order to converge
	'branch_prob': 0.02,

	# Probability of having no child
	'delete_prob': 0.01,

	# Use dictionary to reduce file size
	# While orc does not seem to benefit from it
	'use_dict': False,

	# Default output file name without suffix
	'file_name': 'temp'
}

# Either read config from a .json file or use the default one
def parse_config() -> dict:
	if len(sys.argv) > 1:
		config_file_name = sys.argv[1]
		config = json.load(open(config_file_name))
		for key in default_config:
			if key not in config:
				config[key] = default_config[key]
				print('CAUTION: use %s as value for %s' % (config[key], key), file=sys.stderr)
	else:
		print('Use the default config', file=sys.stderr)
		config = default_config

	return config

def generate_nested(row_count: int, depth: int, config: dict):
	ret = []
	for row_index in range(row_count):
		root = {}
		if config['branch_prob'] != 0:
			_generate_node(root, 0, depth, config)
		else:
			remove_depth = -1 if np.random.rand() >= config['delete_prob'] else 0
			if remove_depth == 0:
				remove_depth = math.floor(np.random.rand() * (depth - 1 - 1e-7))
			_generate_node(root, 0, depth, config, remove_depth)
		ret.append(root)

	return ret

def _generate_node(node: dict, cur_depth: int, total_depth: int, config: dict, remove_depth: int=-1):
	value = _generate_float(config['distinct_float'])
	node['level_%d' % cur_depth] = value

	if cur_depth != total_depth - 1:
		if config['branch_prob'] != 0:
			node['next'] = []
			rand = np.random.rand()
			
			if rand < config['branch_prob']:
				count = 2
			elif rand > 1 - config['delete_prob']:
				count = 0
			else:
				count = 1
			
			for child_index in range(count):
				node['next'].append({})
			for child_index in range(count):
				_generate_node(node['next'][child_index], cur_depth + 1, total_depth, config)
		else:
			if cur_depth != remove_depth:
				node['next'] = {}
				_generate_node(node['next'], cur_depth + 1, total_depth, config, remove_depth)

def _generate_float(distinct: bool):
	if distinct:
		return math.floor(np.random.rand() * 6) / 6
	else:
		return np.random.rand()
