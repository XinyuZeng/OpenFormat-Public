from matplotlib import pyplot as plt

depths = [2, 17, 32, 47, 62]

def main():
	# File size
	file_name = 'python/nested/experiments/depth_query_time/stats_size.txt'
	with open(file_name, 'rt') as file:
		lines = file.readlines()
	
	lines = [[int(block) for block in line.split()] for line in lines]
	pq_size = [size[0] for size in lines]
	assert(len(pq_size) == len(depths))
	
	orc_size = [size[1] for size in lines]
	assert(len(orc_size) == len(depths))

	size_scale = [pq_size[index] / orc_size[index] for index in range(len(depths))]

	plt.plot(depths, size_scale)
	plt.ylabel('depth')
	plt.ylabel('parquet size / orc size')
	plt.savefig('python/nested/experiments/depth_query_time/result_size.png')
	plt.clf()

	# Query time (in Python)
	file_name = 'python/nested/experiments/depth_query_time/stats_time.txt'
	with open(file_name, 'rt') as file:
		lines = file.readlines()
	
	lines = [[float(block) for block in line.split()] for line in lines]
	pq_time = [time[0] for time in lines]
	assert(len(pq_time) == len(depths))
	
	pc_time = [time[1] for time in lines]
	assert(len(pc_time) == len(depths))
	
	orc_time = [time[2] for time in lines]
	assert(len(orc_time) == len(depths))

	plt.plot(depths, pq_time, label='pq')
	plt.plot(depths, pc_time, label='pc')
	plt.plot(depths, orc_time, label='orc')
	plt.ylabel('depth')
	plt.ylabel('time')
	plt.savefig('python/nested/experiments/depth_query_time/result_time.png')
	plt.clf()

if __name__ == '__main__':
	main()