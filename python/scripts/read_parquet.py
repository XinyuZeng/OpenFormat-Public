from parquet_exp import read_parquet
from utils import collect_results, parse_output
import sys, os

if __name__ == "__main__":
    parquet_name = "./{}.parquet".format(sys.argv[1])
    output_stats = {}
    output_stats = read_parquet(parquet_name, output_stats.copy())
    parse_output(output_stats)
    collect_results()
    os.system('mv outputs/stats.csv outputs/{}.csv'.format(sys.argv[1]))