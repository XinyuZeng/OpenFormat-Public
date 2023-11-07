while kill -0 $1 > /dev/null 2>&1; do
    sleep 1
done
send_email $1_finished
