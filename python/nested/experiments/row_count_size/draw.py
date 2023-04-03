from matplotlib import pyplot as plt

row_counts = [16384, 32768, 65536, 131072, 262144]

def main():
	file_name = 'python/nested/experiments/row_count_size/stats.txt'
	with open(file_name, 'rt') as file:
		lines = file.readlines()
	
	lines = [[int(block) for block in line.split()] for line in lines]
	pq_size = [size[0] / (1024 ** 2) for size in lines]
	assert(len(pq_size) == len(row_counts))
	
	orc_size = [size[1] / (1024 ** 2) for size in lines]
	assert(len(orc_size) == len(row_counts))

	plt.plot(row_counts, pq_size, label='Parquet')
	plt.plot(row_counts, orc_size, label='Orc')
	plt.ylabel('row_count')
	plt.ylabel('File size/MB')
	plt.legend()
	plt.savefig('python/nested/experiments/row_count_size/result.png')

if __name__ == '__main__':
	main()