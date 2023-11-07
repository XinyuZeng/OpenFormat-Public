import pyarrow.parquet as pq
import pyarrow.orc as orc
import cudf
import time

from main import clear_cache

def measure(lib: str, path: str, to_clear_cache: bool=True) -> float:
	if to_clear_cache:
		clear_cache()
	if lib == 'pyarrow':
		if path.endswith('.pq'):
			begin = time.time()
			table = pq.read_table(path)
			end = time.time()
		else:
			assert(path.endswith('.orc'))
			begin = time.time()
			table = orc.read_table(path)
			end = time.time()
	else:
		assert(lib == 'cudf')
		if path.endswith('.pq'):
			begin = time.time()
			table = cudf.read_parquet(path)
			end = time.time()
		else:
			assert(path.endswith('.orc'))
			begin = time.time()
			table = cudf.read_orc(path)
			end = time.time()
	return end - begin

if __name__ == '__main__':
	assert(False)
