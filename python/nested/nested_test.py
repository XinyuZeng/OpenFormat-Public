from typing import List
import numpy as np

def main():
	leaves: List[int] = []
	for round in range(65536):
		count = 1
		for depth in range(62):
			new_count = 0
			for index in range(count):
				if np.random.random() > 0.98:
					new_count += 2
				elif np.random.random() > 0.01:
					new_count += 1
			count = new_count
			if count == 0:
				break
		leaves.append(count)
	
	reach = 0
	for leaves_count in leaves:
		if leaves_count != 0:
			reach += 1
	average = sum(leaves) / 65536
	print('Reach count: %d / 65536' % reach)
	print('Reach percentage: %.3f' % (reach / 65536))
	print('Average: %.3f' % average)

if __name__ == '__main__':
	main()
