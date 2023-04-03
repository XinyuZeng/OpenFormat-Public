import pyarrow.orc as po
import pyarrow as pa
from pyarrow import csv
import os
import sys
import time

# Example input: python3 generate_orc.py Generico_1

name = sys.argv[1]

file_name = "./{}.csv".format(name)

# attribute = sys.argv[2:]
attribute = ["Anunciante", "Aviso", "A�o", "Cadena", "Categoria", "Circulacion", "Codigo", "Cols", "Concatenar 1", "Concatenar 2", "Corte", "De_Npags", "Dia_Semana", "Disco", "Duracion", "Est", "FECHA", "Franja", "Genero", "Holding", "Hora_Pagina", "InversionQ",
             "InversionUS", "Marca", "Medio", "Mes", "NumAnuncios", "Number of Records", "Plgs", "Posicion_Edicion", "PrimeraLinea", "Producto", "SEMANA", "Sector", "Soporte", "Subsector", "Unidad", "VER ANUNCIO", "Vehiculo", "extencion", "medio2", "www1", "www2"]
# attribute.append("extra")
# attribute=['s_suppkey','s_name','s_address','s_nationkey','s_phone','s_acctbal','s_comment', 'extra']

# supplier: s_suppkey s_name s_address s_nationkey s_phone s_acctbal s_comment
# lineitem: l_orderkey l_partkey l_suppkey l_linenumber l_quantity l_extendedprice l_discount l_tax l_returnflag l_linestatus l_shipdate l_commitdate l_receiptdate l_shipinstruct l_shipmode l_comment
# part: p_partkey p_name p_mfgr p_brand p_type p_size p_container p_retailprice p_comment
# orders: o_orderkey o_custkey o_orderstatus o_totalprice o_orderdate o_orderpriority o_clerk o_shippriority o_comment

# pd_tbl=pd.read_table(file_name,sep ='|',header=None,names=attribute)
# del pd_tbl['extra']
# print(pd_tbl.head())

# pa_tbl=pa.Table.from_pandas(pd_tbl)

begin = time.time()
pa_tbl = csv.read_csv(file_name, read_options=csv.ReadOptions(
    column_names=attribute), parse_options=csv.ParseOptions(delimiter='|'))
print("read csv time:", time.time() - begin)

begin = time.time()
po.write_table(
    pa_tbl, "./{}_py.orc".format(name)
)
print("write orc time:", time.time() - begin)
