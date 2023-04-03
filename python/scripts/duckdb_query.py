import pyarrow.parquet as pq
import pyarrow.orc as po
import pyarrow as pa
from pyarrow import csv
import sys
import time
import os
from utils import parse_config, enumerate_config, parse_output, collect_results
import duckdb
import pandas as pd

filter_query = """SELECT Anunciante AS Anunciante FROM Generico_1 WHERE ((Anunciante IN ('BANTRAB/TODOTICKET', 'TODOTICKET', 'TODOTICKET.COM')) AND (CAST(EXTRACT(YEAR FROM FECHA) AS BIGINT) >= 2010) AND (CAST(EXTRACT(YEAR FROM FECHA) AS BIGINT) <= 2015)) GROUP BY Anunciante ORDER BY Anunciante ASC ;
"""

# Anunciante
# A�o
# PrimeraLinea
# InversionQ


def run_query(csv_name, col_name):
    # count_query = """SELECT {}, COUNT(*) as count FROM pa_tbl GROUP BY {} ORDER BY count DESC ;""".format(
    #     col_name, col_name)
    count_query = """SELECT AVG(LENGTH({})) as count FROM pa_tbl WHERE {} IS NOT NULL;""".format(
        col_name, col_name)
    # DuckDB
    file = open(csv_name[:-4])
    attribute = [line.strip('\n') for line in file.readlines()]
    file.close()
    pa_tbl = csv.read_csv(csv_name, read_options=csv.ReadOptions(
        column_names=attribute), parse_options=csv.ParseOptions(delimiter='|'))

    # Get database connection
    con = duckdb.connect()

    begin = time.time()
    # Transforms Query Result from DuckDB to Arrow Table
    result = con.execute(count_query).fetch_arrow_table()
    print("query time (s):", time.time() - begin)
    print("arrow table result:")
    print(result.to_string(preview_cols=50))
    con.execute(count_query).fetchdf().to_csv(
        'query_result_{}.csv'.format(col_name), index=False)


def extract_cols(csv_name, col_name):
    file = open(csv_name[:-4])
    attribute = [line.strip('\n') for line in file.readlines()]
    file.close()
    df = pd.read_csv(csv_name, names=attribute, sep='|', engine='pyarrow')
    df[col_name].to_csv('tables/{}.csv'.format(col_name),
                        index=False, header=False)


# usage: python3 scripts/duckdb_query.py [query | extract] [csv_name] [col_name]
# csv_name needs to include tables. e.g tables/Generico_1.csv
if __name__ == "__main__":
    assert len(sys.argv) > 3
    if sys.argv[1] == "query":
        run_query(sys.argv[2], sys.argv[3])
    elif sys.argv[1] == "extract":
        extract_cols(sys.argv[2], sys.argv[3])
    else:
        print(
            "invalid argument\nUsage: python3 duckdb_query.py [query | extract] [csv_name] [col_name]")
