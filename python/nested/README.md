- To run the scripts:
	```shell
	# Run from project directory
	python3 python/nested/generate_file.py
	# To use a config from .json file:
	# python3 python/nested/generate_file.py python/nested/default.json

	# Clear the cache: (require sudo authority)
	sync; echo 3 | sudo tee /proc/sys/vm/drop_caches

	# Test query time:
	python3 python/nested/test_file.py
	# python3 python/nested/test_file.py python/nested/default.json
	```

- To run the defined experiments:
	```shell
	python/nested/experiments/*/run.sh
	```

	Where \* can stand for:

	- `row_count_size`, which shows size vs. row_count

	- `depth_size`, which shows size scale vs. depth

	- `depth_query_time`, which uses Python aggregation to compute `sum` on the deepest column.
	As it's written in Python (the pyarrow interface for Orc does not support directy aggregation on Orc nested column), it's not very useful.

- To draw figures for all experiments:
	```shell
	python/nested/draw_all.sh
	```

	Which will save figures to `python/nested/experiments/*/result*.png`

- As figures are not included in the git repository, use
	```shell
	python/nested/drop_figure.sh
	```

	to `rm` all generated `result*.png`.
	As all `stats*.txt` are not deleted, figures can always be regenerated.

- Generated data

	The `depth` parameter defines the distance from the root node to the deepest leaf node.
	While the case is, not all roots can reach that depth.

	For non-leaf nodes, it can have one or two or zero child in the `next` field.
	Children in this field represent the deeper nested data.

	When generating data, with probability `config['branch_prob']` the node can have two children, while with probability `config['delete_prob']` the node won't contain a child.

	As accessing a nested column across `ListType` in middle steps does not seem to be supported by the current `pyarrow` API, experiment on query time later will use another definition (unimplemented): when `config['branch_prob'] = 0`, the field will simple be `StructType` instead of `ListType`.
	This refers to the `optional` keyword in Dremel's Model instead of `repeated`.

	Probability is defined properly to avoid the case that most of the roots cannot reach the deepest leaf.
