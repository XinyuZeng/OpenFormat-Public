# aws cli
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
rm -rf awscliv2.zip aws
# pip
pip3 install duckdb pandas matplotlib oss2 numpy pyarrow scipy
# arrow-dependency
apt update
apt-get install -y -q --no-install-recommends \
    autoconf \
    ca-certificates \
    ccache \
    cmake \
    g++ \
    gcc \
    gdb \
    git \
    libbenchmark-dev \
    libboost-filesystem-dev \
    libboost-system-dev \
    libbrotli-dev \
    libbz2-dev \
    libc-ares-dev \
    libcurl4-openssl-dev \
    libgflags-dev \
    libgoogle-glog-dev \
    liblz4-dev \
    libprotobuf-dev \
    libprotoc-dev \
    libradospp-dev \
    libre2-dev \
    libsnappy-dev \
    libssl-dev \
    libthrift-dev \
    libutf8proc-dev \
    libzstd-dev \
    make \
    ninja-build \
    nlohmann-json3-dev \
    pkg-config \
    protobuf-compiler \
    python3-dev \
    python3-pip \
    python3-rados \
    rados-objclass-dev \
    rapidjson-dev \
    rsync \
    tzdata \
    wget
# cmake 3.22
apt remove -y cmake
cd /opt
curl -OL https://github.com/Kitware/CMake/releases/download/v3.22.3/cmake-3.22.3-linux-x86_64.sh
chmod +x ./cmake-3.22.3-linux-x86_64.sh
bash ./cmake-3.22.3-linux-x86_64.sh
sudo ln -s /opt/cmake-3.22.3-linux-x86_64/bin/* /usr/bin
# google profiler
# sudo apt-get install -y google-perftools
cd ~
wget https://github.com/gperftools/gperftools/releases/download/gperftools-2.10/gperftools-2.10.tar.gz
tar -xvf gperftools-2.10.tar.gz
cd gperftools-2.10
./configure
make
sudo make install
sudo cp ~/gperftools-2.10/.libs/libprofiler.so /usr/local/lib/