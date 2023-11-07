# most downloaded embeddings: https://huggingface.co/datasets?sort=downloads&search=embed
# https://huggingface.co/datasets/HuggingFaceM4/cm4-synthetic-testing-with-embeddings/tree/refs%2Fconvert%2Fparquet/10k.repeat.embeddings
# substitue "resolve" with "tree" to get the actual viewable url
# wget https://huggingface.co/datasets/HuggingFaceM4/cm4-synthetic-testing-with-embeddings/resolve/refs%2Fconvert%2Fparquet/10k.repeat.embeddings/cm4-synthetic-testing-with-embeddings-train-00000-of-00002.parquet -O cm4-10k-repeat.parquet 
# parquet-layout cm4-10k-repeat.parquet > cm4-10k-repeat.json
# wget https://huggingface.co/datasets/HuggingFaceM4/cm4-synthetic-testing-with-embeddings/resolve/refs%2Fconvert%2Fparquet/10k.unique.embeddings/cm4-synthetic-testing-with-embeddings-train-00000-of-00002.parquet -O cm4-10k-unique.parquet 
# parquet-layout cm4-10k-unique.parquet > cm4-10k-unique.json
# Seems NDV ratio low
# wget https://huggingface.co/datasets/HuggingFaceM4/general-pmd-synthetic-testing-with-embeddings/resolve/refs%2Fconvert%2Fparquet/10k.repeat.embeddings/general-pmd-synthetic-testing-with-embeddings-train.parquet -O general-pmd.parquet 
# parquet-layout general-pmd.parquet > general-pmd.json 

# All the above data sets use row group size = 1000

wget https://huggingface.co/datasets/Cohere/wikipedia-22-12-simple-embeddings/resolve/refs%2Fconvert%2Fparquet/Cohere--wikipedia-22-12-simple-embeddings/parquet-train-00000-of-00004.parquet -O cohere-simple.parquet 
parquet-layout cohere-simple.parquet > cohere-simple.json 
# wget https://huggingface.co/datasets/Cohere/wikipedia-22-12-en-embeddings/resolve/refs%2Fconvert%2Fparquet/Cohere--wikipedia-22-12-en-embeddings/parquet-train-00000-of-00002.parquet -O cohere-en.parquet
# parquet-layout cohere-en.parquet > cohere-en.json

# The above cohere's datasets no longer have low NDV ratios.

# wget https://huggingface.co/datasets/DoyyingFace/github-embeddings-doy/resolve/refs%2Fconvert%2Fparquet/DoyyingFace--github-embeddings-doy/json-train.parquet -O github-embeddings-doy.parquet 
# parquet-layout github-embeddings-doy.parquet > github-embeddings-doy.json 
# This also have high NDV ratio.
# snappy does not help anything.

wget https://huggingface.co/datasets/llm-book/jawiki-20220404-c400-large-with-bpr-embeddings/resolve/main/data/train-00000-of-00007-97deefc5b1d142b9.parquet -O llm-book.parquet  
parquet-layout llm-book.parquet > llm-book.json 
# low again. 

wget https://huggingface.co/datasets/tollefj/rettsavgjoerelser_100samples_embeddings/resolve/refs%2Fconvert%2Fparquet/tollefj--rettsavgjoerelser_100samples_embeddings/parquet-train.parquet -O tollefj.parquet 
parquet-layout tollefj.parquet > tollefj.json 
# high again.

# clip_l14_embedding
wget https://huggingface.co/datasets/nielsr/datacomp-small-with-embeddings-and-cluster-labels/resolve/refs%2Fconvert%2Fparquet/nielsr--datacomp-small-with-embeddings-and-cluster-labels/parquet-train-00000-of-00176.parquet -O nielsr--datacomp-small-with-embeddings-and-cluster-labels.parquet 
parquet-layout nielsr--datacomp-small-with-embeddings-and-cluster-labels.parquet > nielsr--datacomp-small-with-embeddings-and-cluster-labels.json 
# this is on page 2

# Tax_embeddings
wget https://huggingface.co/datasets/vjain/tax_embeddings/resolve/main/Tax_embeddings.parquet -O vjain--tax_embedding.parquet
parquet-layout vjain--tax_embedding.parquet > vjain--tax-embedding.json 

# AP_bio_embedding
# wget https://huggingface.co/datasets/vjain/biology_AP_embeddings/resolve/main/AP_bio_embeddings.parquet -O vjain--AP_bio_embedding.parquet
# parquet-layout vjain--AP_bio_embedding.parquet > vjain--AP_bio_embedding.json 

# Cihere_wiki_ko
# wget https://huggingface.co/datasets/Cohere/wikipedia-22-12-ko-embeddings/resolve/main/data/train-00000-of-00010-a06e06311b69ac22.parquet -O Cohere--ko_embedding.parquet 
# parquet-layout Cohere--ko_embedding.parquet > Cohere--ko_embedding.json 

# AP_phy
# wget https://huggingface.co/datasets/vjain/AP_physics_embeddings/resolve/main/AP_Physics_embeddings.parquet -O vjain--AP_phy_embedding.parquet
# parquet-layout vjain--AP_phy_embedding.parquet > vjain--AP_phy_embedding.json 

# Cohere_wiki_es
# wget https://huggingface.co/datasets/Cohere/wikipedia-22-12-es-embeddings/resolve/main/data/train-00000-of-00073-da9bdcef23cace12.parquet -O Cohere--es_embedding.parquet  
# parquet-layout Cohere--es_embedding.parquet > Cohere--es_embedding.json 

# # Cohere_wiki_ar
# wget https://huggingface.co/datasets/Cohere/wikipedia-22-12-ar-embeddings/resolve/main/data/train-00000-of-00024-de6836d521cb0363.parquet -O Cohere--ar_embedding.parquet  
# parquet-layout Cohere--ar_embedding.parquet > Cohere--ar_embedding.json 

# # Cohere_wiki_fr
# wget https://huggingface.co/datasets/Cohere/wikipedia-22-12-fr-embeddings/resolve/main/data/train-00000-of-00095-94fb2c4d1a93fc03.parquet -O Cohere--fr_embedding.parquet 
# paruet-layout Cohere--fr_embedding.parquet > Cohere--fr_embedding.json 

# davanstrien_ia
# wget https://huggingface.co/datasets/davanstrien/ia-loaded-embedded-gpu/resolve/main/data/train-00001-of-00004-d74d2d728c7093fa.parquet -O davanstrein--ia_loaded.parquet 
# parquet-layout davanstrein--ia_loaded.parquet > davanstrein--ia_loaded.json 

# Cohere_wiki_zh
# wget https://huggingface.co/datasets/Cohere/wikipedia-22-12-zh-embeddings/resolve/main/data/train-00000-of-00017-20a9866562d051fb.parquet -O Cohere--zh_embedding.parquet 
# parquet-layout Cohere--zh_embedding.parquet > Cohere--zh_embedding.json 

# murphyk_dogs_cats
wget https://huggingface.co/datasets/Maiia/mcphrasy_test_skill_tok_embed/resolve/main/data/train-00000-of-00020-dacdd36e919134e8.parquet -O maiia--mcphrasy.parquet
parquet-layout maiia--mcphrasy.parquet > maiia--mcphrasy.json 

# dhmeltzer_ELI5
wget https://huggingface.co/datasets/dhmeltzer/ELI5_embedded/resolve/main/data/train-00000-of-00005-b9fc9ed92113a2b9.parquet -O dhmeltzer--eli5.parquet 
parquet-layout dhmeltzer--eli5.parquet > dhmeltzer--eli5.json 

# crumb_fan_t5, this is 1.4G large, perhaps need to omit
wget https://huggingface.co/datasets/crumb/flan-t5-small-embed-refinedweb/resolve/main/data/00.parquet -O crumb--flan_t5.parquet 
parquet-layout crumb--flan_t5.parquet > crumb--flan_t5.json 


# LukeSajkowski_products
wget https://huggingface.co/datasets/LukeSajkowski/products_ecommerce_embeddings/resolve/main/data/train-00000-of-00001-e105819ae8d912bc.parquet -O LukeSajkowski--eco.parquet 
parquet-layout LukeSajkowski--eco.parquet > LukeSajkowski--eco.json 

# KShivendu_openai
wget https://huggingface.co/datasets/KShivendu/wikipedia-1k-cohere-openai-embeddings/resolve/main/data/train-00000-of-00001-aec4da7891cd3048.parquet -O KShivendu--openai.parquet 
parquet-layout KShivendu--openai.parquet > KShivendu--openai.json 

# lsb_minilm
wget https://huggingface.co/datasets/lsb/simplewiki2023-all-minilm-l6-v2-embedding/resolve/main/data/train-00000-of-00002-3dabc2a2d6e3d70d.parquet -O lsb--minilm.parquet 
parquet-layout lsb--minilm.parquet > lsb--minilm.json 


## Recent
# clip_l14_embedding
wget https://huggingface.co/datasets/nielsr/datacomp-small-with-embeddings-and-cluster-labels/resolve/refs%2Fconvert%2Fparquet/nielsr--datacomp-small-with-embeddings-and-cluster-labels/parquet-train-00000-of-00176.parquet -O nielsr--datacomp-small-with-embeddings-and-cluster-labels.parquet 
parquet-layout nielsr--datacomp-small-with-embeddings-and-cluster-labels.parquet > nielsr--datacomp-small-with-embeddings-and-cluster-labels.json 

# encoding
# wget https://huggingface.co/datasets/crumb/flan-t5-small-embed-refinedweb/resolve/refs%2Fconvert%2Fparquet/crumb--flan-t5-small-embed-refinedweb/parquet-train-00000-of-00032.parquet -O crumb--flan-t5-small-embed-refinedweb.parquet 
# parquet-layout crumb--flan-t5-small-embed-refinedweb.parquet > crumb--flan-t5-small-embed-refinedweb.json 

# encoding 
# wget https://huggingface.co/datasets/crumb/flan-t5-large-embed-refinedweb/resolve/refs%2Fconvert%2Fparquet/crumb--flan-t5-large-embed-refinedweb/parquet-train-00000-of-00064.parquet -O crumb--flan-t5-large-embed-refinedweb.parquet 
# parquet-layout crumb--flan-t5-large-embed-refinedweb.parquet > crumb--flan-t5-large-embed-refinedweb.json 

# test
wget https://huggingface.co/datasets/xwjzds/ag_news_embed_test/resolve/refs%2Fconvert%2Fparquet/xwjzds--ag_news_embed_test/parquet-train.parquet -O xwjzds--ag_news_embed_test.parquet 
parquet-layout xwjzds--ag_news_embed_test.parquet > xwjzds--ag_news_embed_test.json 

# embedding
wget https://huggingface.co/datasets/justinian336/salvadoran-news-embedded/resolve/refs%2Fconvert%2Fparquet/justinian336--salvadoran-news-embedded/parquet-train-00000-of-00002.parquet -O justinian336--salvadoran-news-embedded.parquet 
parquet-layout justinian336--salvadoran-news-embedded.parquet > justinian336--salvadoran-news-embedded.json 

# embeddings
wget https://huggingface.co/datasets/davanstrien/ia_embedded/resolve/refs%2Fconvert%2Fparquet/davanstrien--ia_embedded/parquet-train-00000-of-00002.parquet -O davanstrien--ia_embedded.parquet 
parquet-layout davanstrien--ia_embedded.parquet > davanstrien--ia_embedded.json 

# embeddings
# wget https://huggingface.co/datasets/dhmeltzer/ELI5_embedded/resolve/refs%2Fconvert%2Fparquet/dhmeltzer--ELI5_embedded/parquet-train-00000-of-00005.parquet -O dhmeltzer--ELI5_embedded.parquet 
# parquet-layout dhmeltzer--ELI5_embedded.parquet > dhmeltzer--ELI5_embedded.json 

# mean_embedding, cls_embedding
wget https://huggingface.co/datasets/sanjin7/embedding_dataset_distilbert_base_uncased_ad_subwords/resolve/refs%2Fconvert%2Fparquet/sanjin7--embedding_dataset_distilbert_base_uncased_ad_subwords/parquet-train.parquet -O sanjin7--embedding_dataset_distilbert_base_uncased_ad_subwords.parquet 
parquet-layout sanjin7--embedding_dataset_distilbert_base_uncased_ad_subwords.parquet > sanjin7--embedding_dataset_distilbert_base_uncased_ad_subwords.json 

# l14_embeddings, moco_vitb_imagenet_embeddings, moco_vitb_imagenet_embeddings_without_last_layer
# wget https://huggingface.co/datasets/Isamu136/big-animal-dataset-with-embedding/resolve/refs%2Fconvert%2Fparquet/Isamu136--big-animal-dataset-with-embedding/parquet-train-00002-of-00005.parquet -O Isamu136--big-animal-dataset-with-embedding.parquet
# parquet-layout Isamu136--big-animal-dataset-with-embedding.parquet > Isamu136--big-animal-dataset-with-embedding.json

# embedding
wget https://huggingface.co/datasets/lsb/openwebtext-all-minilm-l6-v2-embedding/resolve/refs%2Fconvert%2Fparquet/lsb--openwebtext-all-minilm-l6-v2-embedding/parquet-train-00000-of-00105.parquet -O lsb--openwebtext-all-minilm-l6-v2-embedding.parquet 
parquet-layout lsb--openwebtext-all-minilm-l6-v2-embedding.parquet > lsb--openwebtext-all-minilm-l6-v2-embedding.json 

#
# wget https://huggingface.co/datasets/rocca/clip-keyphrase-embeddings/resolve/refs%2Fconvert%2Fparquet/rocca--clip-keyphrase-embeddings/csv-train-00000-of-00004.parquet -O rocca--clip-keyphrase-embeddings.parquet
# parquet-layout rocca--clip-keyphrase-embeddings.parquet > rocca--clip-keyphrase-embeddings.json

# embeddings
# wget https://huggingface.co/datasets/flxclxc/cellock_data_with_embeddings/resolve/refs%2Fconvert%2Fparquet/flxclxc--cellock_data_with_embeddings/parquet-train.parquet -O flxclxc--cellock_data_with_embeddings.parquet
# parquet-layout flxclxc--cellock_data_with_embeddings.parquet > flxclxc--cellock_data_with_embeddings.json

# embedding
wget https://huggingface.co/datasets/davanstrien/pytorchmodelmetadata_with_embeddings/resolve/refs%2Fconvert%2Fparquet/davanstrien--pytorchmodelmetadata_with_embeddings/parquet-train.parquet -O davanstrien--pytorchmodelmetadata_with_embeddings.parquet 
parquet-layout davanstrien--pytorchmodelmetadata_with_embeddings.parquet > davanstrien--pytorchmodelmetadata_with_embeddings.json 

# wget https://huggingface.co/datasets/davanstrien/test_imdb_embedd2/resolve/main/data/test-00000-of-00001-a24281c0d6f704cf.parquet -O davanstrien--imdb_embedded.parquet 
# parquet-layout davanstrien--imdb_embedded.parquet > davanstrien--imdb_embedded.json 

# wget https://huggingface.co/datasets/Maiia/mcphrasy_test_skill_tok_embed/resolve/main/data/train-00000-of-00020-dacdd36e919134e8.parquet -O maiia--test_skill_tok_embed.parquet
# parquet-layout maiia--test_skill_tok_embed.parquet > maiia--test_skill_tok_embed.json

# wget https://huggingface.co/datasets/davanstrien/blbooks-parquet-embedded/resolve/main/data/train-00000-of-00001-ecfc49a85bbf2d3c.parquet -O davanstrien--blbooks_parquet_embedded.parquet 
# parquet-layout davanstrien--blbooks_parquet_embedded.parquet > davanstrien--blbooks_parquet_embedded.json 

# wget https://huggingface.co/datasets/davanstrien/ia-loaded-embedded-gpu/resolve/main/data/train-00000-of-00004-581edf813b59d1d8.parquet -O davastrien--ia-gpu.parquet 
# parquet-layout davastrien--ia-gpu.parquet > davastrien--ia-gpu.json 

# wget https://huggingface.co/datasets/davanstrien/test_imdb_embedd1/resolve/main/data/train-00000-of-00001-66144b8917e3580d.parquet -O davanstrien--test_imdb_embedd1.parquet 
# parquet-layout davanstrien--test_imdb_embedd1.parquet > davanstrien--test_imdb_embedd1.json 

wget https://huggingface.co/datasets/victorych22/lamini-embedded-instructions-only/resolve/main/data/train-00000-of-00011-1017a725fc1eba59.parquet -O victorych22--lamini.parquet 
parquet-layout victorych22--lamini.parquet > victorych22--lamini.json 

wget https://huggingface.co/datasets/rjac/all-the-news-2-1-Component-one-embedding/resolve/main/data/train-00003-of-00006-6cd2d007a4ed7d24.parquet -O rjac--all_the_news.parquet 
parquet-layout rjac--all_the_news.parquet > rjac--all_the_news.json 

wget https://huggingface.co/datasets/KaiserML/SemanticScholar_all-distilroberta-v1_Embeddings/resolve/main/data/train-00000-of-00002-45f8ae2d1623543b.parquet -O KaiserML--SemanticScholar.parquet 
parquet-layout KaiserML--SemanticScholar.parquet > KaiserML--SemanticScholar.json 

wget https://huggingface.co/datasets/Multimodal-Fatima/StanfordCars_train_embeddings/resolve/main/data/openai_clip_vit_large_patch14-00000-of-00003-9cf553abd83b0312.parquet -O Multimodal-Fatima--StanfordCars_train_embeddings.parquet
parquet-layout Multimodal-Fatima--StanfordCars_train_embeddings.parquet > Multimodal-Fatima--StanfordCars_train_embeddings.json

wget https://datasets-documentation.s3.eu-west-3.amazonaws.com/laion/0001.parquet -O laion.parquet