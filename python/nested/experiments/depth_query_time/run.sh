# python3 python/nested/generate_file.py python/nested/experiments/depth_query_time/2.json
# python3 python/nested/generate_file.py python/nested/experiments/depth_query_time/17.json
# python3 python/nested/generate_file.py python/nested/experiments/depth_query_time/32.json
# python3 python/nested/generate_file.py python/nested/experiments/depth_query_time/47.json
# python3 python/nested/generate_file.py python/nested/experiments/depth_query_time/62.json

# rm python/nested/experiments/depth_query_time/stats_size.txt
# python3 python/nested/get_file_size.py python/nested/experiments/depth_query_time/2 >> python/nested/experiments/depth_query_time/stats_size.txt
# python3 python/nested/get_file_size.py python/nested/experiments/depth_query_time/17 >> python/nested/experiments/depth_query_time/stats_size.txt
# python3 python/nested/get_file_size.py python/nested/experiments/depth_query_time/32 >> python/nested/experiments/depth_query_time/stats_size.txt
# python3 python/nested/get_file_size.py python/nested/experiments/depth_query_time/47 >> python/nested/experiments/depth_query_time/stats_size.txt
# python3 python/nested/get_file_size.py python/nested/experiments/depth_query_time/62 >> python/nested/experiments/depth_query_time/stats_size.txt

rm python/nested/experiments/depth_query_time/stats_time.txt
python3 python/nested/test_file.py python/nested/experiments/depth_query_time/2.json >> python/nested/experiments/depth_query_time/stats_time.txt
python3 python/nested/test_file.py python/nested/experiments/depth_query_time/17.json >> python/nested/experiments/depth_query_time/stats_time.txt
python3 python/nested/test_file.py python/nested/experiments/depth_query_time/32.json >> python/nested/experiments/depth_query_time/stats_time.txt
python3 python/nested/test_file.py python/nested/experiments/depth_query_time/47.json >> python/nested/experiments/depth_query_time/stats_time.txt
python3 python/nested/test_file.py python/nested/experiments/depth_query_time/62.json >> python/nested/experiments/depth_query_time/stats_time.txt

python3 python/nested/experiments/depth_query_time/draw.py
