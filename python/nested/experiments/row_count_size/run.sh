python3 python/nested/generate_file.py python/nested/experiments/row_count_size/16384.json
python3 python/nested/generate_file.py python/nested/experiments/row_count_size/32768.json
python3 python/nested/generate_file.py python/nested/experiments/row_count_size/65536.json
python3 python/nested/generate_file.py python/nested/experiments/row_count_size/131072.json
python3 python/nested/generate_file.py python/nested/experiments/row_count_size/262144.json

rm python/nested/experiments/row_count_size/stats.txt
python3 python/nested/get_file_size.py python/nested/experiments/row_count_size/16384 >> python/nested/experiments/row_count_size/stats.txt
python3 python/nested/get_file_size.py python/nested/experiments/row_count_size/32768 >> python/nested/experiments/row_count_size/stats.txt
python3 python/nested/get_file_size.py python/nested/experiments/row_count_size/65536 >> python/nested/experiments/row_count_size/stats.txt
python3 python/nested/get_file_size.py python/nested/experiments/row_count_size/131072 >> python/nested/experiments/row_count_size/stats.txt
python3 python/nested/get_file_size.py python/nested/experiments/row_count_size/262144 >> python/nested/experiments/row_count_size/stats.txt

python3 python/nested/experiments/row_count_size/draw.py
