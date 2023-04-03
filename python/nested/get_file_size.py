import os
import sys

def main():
	file_name = sys.argv[1]
	print(os.path.getsize('%s.parquet' % file_name), os.path.getsize('%s.orc' % file_name))

if __name__ == '__main__':
	main()