# /opt/nvidia/nsight-systems/2023.2.3/bin/nsys profile -t nvtx,cuda,osrt -f true --cuda-memory-usage=true --output=nsight/$1 python3 main.py $1.txt
python3 main.py $1.txt
