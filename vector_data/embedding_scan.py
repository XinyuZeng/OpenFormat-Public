# %%
import zarr
import h5py
import pyarrow.parquet as pq
import pyarrow.orc as po
import pyarrow as pa
import numpy as np
import os
import sys
import datetime
import pathlib
import pandas as pd

dir_path = pathlib.Path(os.path.abspath('')).resolve()
HOME_DIR = str(dir_path).split('/OpenFormat')[0]

timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

PROJ_SRC_DIR = f'{HOME_DIR}/OpenFormat'
sys.path.insert(1, f'{PROJ_SRC_DIR}')
from python.scripts.utils import *
print(dir_path)

# file_to_embed_col = {
#                     'nielsr--datacomp-small-with-embeddings-and-cluster-labels.parquet' : ['clip_l14_embedding'],
#                      'crumb--flan-t5-small-embed-refinedweb.parquet': ['encoding'],
#                      'crumb--flan-t5-large-embed-refinedweb.parquet': ['encoding'],
#                      'xwjzds--ag_news_embed_test.parquet': ['test'],
#                      'justinian336--salvadoran-news-embedded.parquet': ['embedding'],
#                      'davanstrien--ia_embedded.parquet': ['embeddings'],
#                      'dhmeltzer--ELI5_embedded.parquet': ['embeddings'],
#                      'sanjin7--embedding_dataset_distilbert_base_uncased_ad_subwords.parquet': ['mean_embedding'],
#                      'lsb--openwebtext-all-minilm-l6-v2-embedding.parquet': ['embedding'],
#                      'davanstrien--pytorchmodelmetadata_with_embeddings.parquet': ['embedding'],
#                      'llm-book.parquet': ['embeddings'],
#                      'tollefj.parquet': ['embedding'],
#                      'github-embeddings-doy.parquet': ['embeddings'],
#                      'cohere-en.parquet': ['emb'],
#                      'cohere-simple.parquet': ['emb'],
#                      'laion.parquet': ['text_embedding'],
#                      }
file_to_embed_col = {
    'cohere-simple': ['emb'],
    'crumb--flan_t5': ['encoding'],
    'davanstrien--ia_embedded': ['embeddings'],
    'davanstrien--pytorchmodelmetadata_with_embeddings': ['embedding'],
    'dhmeltzer--eli5': ['embeddings'],
    'justinian336--salvadoran-news-embedded': ['embedding'],
    'KaiserML--SemanticScholar': ['embedding'],
    'KShivendu--openai': ['openai'],
    'laion': ['text_embedding'],
    'llm-book': ['embeddings'],
    'lsb--minilm': ['embedding'],
    'lsb--openwebtext-all-minilm-l6-v2-embedding': ['embedding'],
    'LukeSajkowski--eco': ['embeddings'],
    'maiia--mcphrasy': ['embeddings'],
    'Multimodal-Fatima--StanfordCars_train_embeddings': ['vision_embeddings'],
    'nielsr--datacomp-small-with-embeddings-and-cluster-labels': ['clip_l14_embedding'],
    'rjac--all_the_news': ['embedding'],
    'sanjin7--embedding_dataset_distilbert_base_uncased_ad_subwords': ['mean_embedding'],
    # 'tollefj': ['embedding'],
    'victorych22--lamini': ['embeddings'],
    'vjain--tax_embedding': ['embedding'],
    'xwjzds--ag_news_embed_test': ['test']
}

yp_fnames = [
    'mushfirat--diffusiondb', # https://www.kaggle.com/datasets/mushfirat/diffusiondb-metadata-with-prompt-embeddings
    'alpayariyak--math_embedded', # https://huggingface.co/datasets/alpayariyak/MATH_Embedded_Instructor-XL/tree/main
    'milly233--gov_data', # https://huggingface.co/datasets/milly233/govdatata_embedding/tree/main
    'CShorten--ArXivML', # https://huggingface.co/datasets/CShorten/ArXiv-ML-Abstract-Embeddings/tree/main
    'juancopi81--yannic', # https://huggingface.co/datasets/juancopi81/yannic_ada_embeddings/tree/main/data
    'Multimodal-Fatima--VQAv2', # https://huggingface.co/datasets/Multimodal-Fatima/VQAv2_sample_test_embeddings/tree/main
    'Manzorq--hf_spsaces', # https://huggingface.co/datasets/anzorq/hf-spaces-descriptions-embeddings
    'nickmuchi--netflix', # https://huggingface.co/datasets/nickmuchi/netflix-shows-mpnet-embeddings
    'mitermix--mj5_clip' #https://huggingface.co/datasets/mitermix/mj5-clip-l-14-oai-embeddings/tree/main
    # 'danofer--swp_t5',  # https://www.kaggle.com/datasets/danofer/uniprotkbswiss-prot-protein-embeddings?select=SWP_perProtein_T5_embed.parquet
]


# %%
# os.system('scp -r aws-cn-i3:/mnt/OpenFormat/vector_data/hug_data_embed_only/ ./')

# %%
output_stats = {}
# os.system('sync; echo 3 > /proc/sys/vm/drop_caches')
os.system("rm outputs/stats.json")
for fname in list(file_to_embed_col.keys()) + yp_fnames:
    for i in range(3):
        # embed_name = embed_name[0]
        output_stats['fname'] = fname
        output_stats['i'] = i
        os.system('sync; echo 3 > /proc/sys/vm/drop_caches')

        begin = time.time()
        loaded_numpy_array = np.load(f'hug_data_embed_only/{fname}.npy', allow_pickle=True)
        output_stats['numpy'] = time.time() - begin
        del loaded_numpy_array
        
        begin = time.time()
        loaded_numpy_array = np.load(f'hug_data_embed_only/{fname}.v.npy', allow_pickle=True)
        output_stats['numpy-v'] = time.time() - begin
        del loaded_numpy_array

        begin = time.time()
        table = po.read_table(f'hug_data_embed_only/{fname}.zstd.orc', columns=['embedding'])
        output_stats['orc-read-table'] = time.time() - begin
        orc_inmem = table.column('embedding').to_numpy()
        orc_time = time.time() - begin
        output_stats['orc-zstd'] = orc_time
        del table, orc_inmem
        
        
        # begin = time.time()
        # table = pq.read_table(f'hug_data_embed_only/{fname}.parquet', columns=['embedding'])
        # pq_inmem = table.column('embedding').to_numpy()
        # pq_time = time.time() - begin
        # output_stats['fname'] = fname
        # output_stats['pq_time'] = pq_time
        
        begin = time.time()
        table = pq.read_table(f'hug_data_embed_only/{fname}.zstd.parquet', columns=['embedding'])
        output_stats['parquet-read-table'] = time.time() - begin
        pq_inmem = table.column('embedding').to_numpy()
        pq_time = time.time() - begin
        output_stats['parquet-zstd'] = pq_time
        # del table, pq_inmem
        
        # begin = time.time()
        # znp = zarr.load(f'hug_data_embed_only/{fname}.zarr')['embedding']
        # output_stats['zarr_time'] = time.time() - begin
        
        
        begin = time.time()
        znp = zarr.load(f'hug_data_embed_only/{fname}.zstd.zarr')['embedding']
        output_stats['zarr-zstd'] = time.time() - begin
        del znp

        # begin = time.time()
        # znp = zarr.load(f'hug_data_embed_only/{fname}.pqchunk.zstd.zarr')['embedding']
        # output_stats['zarr-zstd-pqchunk'] = time.time() - begin

        begin = time.time()
        znp = zarr.load(f'hug_data_embed_only/{fname}.zstd-level1-bitshuffle.zarr')['embedding']
        output_stats['zarr-zstd-level1-bitshuffle'] = time.time() - begin
        del znp
        
        begin = time.time()
        h5f = h5py.File(f'hug_data_embed_only/{fname}.gzip.h5')
        numpy_array = np.array(h5f['embedding'])
        del h5f, numpy_array
        
        output_stats['hdf5-gzip'] = time.time() - begin
        os.makedirs(os.path.dirname("outputs/stats.json"), exist_ok=True)
        stats = open("outputs/stats.json", 'a+')
        stats.write(json.dumps(output_stats)+"\n")
        stats.close()
collect_results()
os.system('mv outputs/stats.csv outputs/hpc_scan_'+timestamp+'.csv')
os.system('send_email hdft_test2')
# %%
