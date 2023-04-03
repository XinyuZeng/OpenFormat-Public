mkdir result/compression_level/TABLE
echo "--------------- Experiment for TABLE round 0 ---------------"
python3 scripts/parquet_exp.py TABLE parquet_none
python3 scripts/parquet_exp.py TABLE parquet_gzip
python3 scripts/parquet_exp.py TABLE parquet_lz4
python3 scripts/parquet_exp.py TABLE parquet_zstd
mkdir result/compression_level/TABLE/0
mv outputs/parquet_none.csv outputs/parquet_gzip.csv outputs/parquet_lz4.csv outputs/parquet_zstd.csv result/compression_level/TABLE/0
echo "--------------- Experiment for TABLE round 1 ---------------"
python3 scripts/parquet_exp.py TABLE parquet_none
python3 scripts/parquet_exp.py TABLE parquet_gzip
python3 scripts/parquet_exp.py TABLE parquet_lz4
python3 scripts/parquet_exp.py TABLE parquet_zstd
mkdir result/compression_level/TABLE/1
mv outputs/parquet_none.csv outputs/parquet_gzip.csv outputs/parquet_lz4.csv outputs/parquet_zstd.csv result/compression_level/TABLE/1
echo "--------------- Experiment for TABLE round 2 ---------------"
python3 scripts/parquet_exp.py TABLE parquet_none
python3 scripts/parquet_exp.py TABLE parquet_gzip
python3 scripts/parquet_exp.py TABLE parquet_lz4
python3 scripts/parquet_exp.py TABLE parquet_zstd
mkdir result/compression_level/TABLE/2
mv outputs/parquet_none.csv outputs/parquet_gzip.csv outputs/parquet_lz4.csv outputs/parquet_zstd.csv result/compression_level/TABLE/2
