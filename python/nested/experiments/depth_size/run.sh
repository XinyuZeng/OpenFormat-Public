dep_list=(2 4 8 17 32 47 62)
for dep in ${dep_list[@]}
do
    python3 python/nested/generate_file.py python/nested/experiments/depth_size/$dep.json
done

rm python/nested/experiments/depth_size/stats.txt
for dep in ${dep_list[@]}
do
    python3 python/nested/get_file_size.py python/nested/experiments/depth_size/$dep >> python/nested/experiments/depth_size/stats.txt
done

# python3 python/nested/experiments/depth_size/draw.py
