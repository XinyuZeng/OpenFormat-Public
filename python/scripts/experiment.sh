echo "begin time: $(date)"
sync; echo 3 > /proc/sys/vm/drop_caches # drop mem cache
$@ &
pid=$!
iostat 5 > io_profile.log &
mpstat 5 > cpu_profile.log &
wait $pid
pkill -2 mpstat
pkill -2 iostat
# python3 scripts/send_email.py experiment
echo "finish time: $(date)"
