from typing import Dict
import os
import pyarrow as pa
import pyarrow.orc as orc
import pyarrow.parquet as pq
import pyarrow.csv as csv
import json

pq_default = {
	'compression': 'SNAPPY',
	'use_dictionary': True,
	'dictionary_pagesize_limit': 1048576,
	'row_group_size': 1048576,
	'version': '2.6',
	'data_page_version': '2.0'
}
orc_default = {
	'compression': 'SNAPPY',
	'compression_strategy': 'speed',
	'dictionary_key_size_threshold': 0.8
}

def add_setting(properties: Dict, name: str):
	data = pq.read_table('pq/%s.pq' % name)
	_save_setting(data, properties, name)

def save_customize(data: pa.Table, properties: Dict, name: str, pq_dict: Dict, orc_dict: Dict):
	pq.write_table(data, 'pq/%s.pq' % name,
		**pq_dict
	)
	orc.write_table(data, 'orc/%s.orc' % name,
		**orc_dict
	)
	_save_setting(data, properties, name)

def save_file_above_default(data: pa.Table, file_type: str, name: str, args: Dict) -> str:
	if file_type == 'pq':
		default_dict = pq_default
	else:
		assert(file_type == 'orc')
		default_dict = orc_default

	for key in default_dict:
		if key not in args:
			args[key] = default_dict[key]
			
	if file_type == 'pq':
		pq.write_table(data, 'pq/%s.pq' % name, **args)
		return 'pq/%s.pq' % name
	else:
		assert(file_type == 'orc')
		orc.write_table(data, 'orc/%s.orc' % name, **args)
		return 'orc/%s.orc' % name

def save(data: pa.Table, properties: Dict, name: str):
	return save_customize(data, properties, name, pq_default, orc_default)

def save_from_csv(path: str, properties: Dict, name: str):
	data = csv.read_csv(path)
	save(data, properties, name)

def save_orc_partial_setting(data: pa.Table, properties: Dict, name: str):
	orc_file = orc.ORCFile('orc/%s.orc' % name)
	if os.path.exists('setting/%s' % name):
		setting = json.load(open('setting/%s' % name, 'rt'))
	else:
		setting = {
			'row_count': len(data)
		}
	setting['orc_size'] = os.path.getsize('orc/%s.orc' % name)
	setting['orc_num_stripes'] = orc_file.nstripes
	for property_name in properties:
		assert(property_name not in {'row_count', 'pq_size', 'orc_size', 'pq_num_row_groups'})
		setting[property_name] = properties[property_name]
	json.dump(setting, open('setting/%s' % name, 'wt'))

def save_orc_setting(data: pa.Table, properties: Dict, name: str):
	orc_file = orc.ORCFile('orc/%s.orc' % name)
	setting = {
		'row_count': len(data),
		'pq_size': 0,
		'pq_num_row_groups': 0,
		'orc_size': os.path.getsize('orc/%s.orc' % name),
		'orc_num_stripes': orc_file.nstripes
	}
	for property_name in properties:
		assert(property_name not in {'row_count', 'pq_size', 'orc_size', 'pq_num_row_groups'})
		setting[property_name] = properties[property_name]
	json.dump(setting, open('setting/%s' % name, 'wt'))

def save_pq_partial_setting(data: pa.Table, properties: Dict, name: str):
	pq_file = pq.ParquetFile('pq/%s.pq' % name)
	if os.path.exists('setting/%s' % name):
		setting = json.load(open('setting/%s' % name, 'rt'))
	else:
		setting = {
			'row_count': len(data)
		}
	setting['pq_size'] = os.path.getsize('pq/%s.pq' % name)
	setting['pq_num_row_groups'] = pq_file.num_row_groups
	for property_name in properties:
		assert(property_name not in {'row_count', 'pq_size', 'orc_size', 'pq_num_row_groups'})
		setting[property_name] = properties[property_name]
	json.dump(setting, open('setting/%s' % name, 'wt'))

def save_pq_setting(data: pa.Table, properties: Dict, name: str):
	pq_file = pq.ParquetFile('pq/%s.pq' % name)
	setting = {
		'row_count': len(data),
		'pq_size': os.path.getsize('pq/%s.pq' % name),
		'pq_num_row_groups': pq_file.num_row_groups,
		'orc_size': 0,
		'orc_num_stripes': 0
	}
	for property_name in properties:
		assert(property_name not in {'row_count', 'pq_size', 'orc_size', 'pq_num_row_groups'})
		setting[property_name] = properties[property_name]
	json.dump(setting, open('setting/%s' % name, 'wt'))

def _save_setting(data: pa.Table, properties: Dict, name: str):
	pq_file = pq.ParquetFile('pq/%s.pq' % name)
	orc_file = orc.ORCFile('orc/%s.orc' % name)
	setting = {
		'row_count': len(data),
		'pq_size': os.path.getsize('pq/%s.pq' % name),
		'pq_num_row_groups': pq_file.num_row_groups,
		'orc_size': os.path.getsize('orc/%s.orc' % name),
		'orc_num_stripes': orc_file.nstripes
	}
	for property_name in properties:
		assert(property_name not in {'row_count', 'pq_size', 'orc_size', 'pq_num_row_groups'})
		setting[property_name] = properties[property_name]
	json.dump(setting, open('setting/%s' % name, 'wt'))

if __name__ == '__main__':
	assert(False)
