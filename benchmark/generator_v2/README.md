# New Generator
## Quick Start
Use `gen_workloads.py` to generate dataset:
```sh
python3 gen_workloads.py <workload> <row_num> <col_num> <target_basefile_name>
# example:
python3 gen_workloads.py core 1000000 30 tmp
```
In `<workload>` option, you can use one of the `core, multiple, classic, geo, log, ml`, where `multiple` will generate the last four datasets in one pass.

The generated dataset file will be placed in folder `./{target_basefile_name}/gen_data`, named `{target_basefile_name}.csv`

## Detailed Executing Logic
In folder `./workload_config`, we have consolidated stats-config for each workload. This configs are extracted from realdata features in `../../feature/outputs/stats.csv`, you can re-extract the workload-config by running `python core_config_gen.py` and `python multi_workloads_config_gen.py`.

File `generator.py` provides a Generator class, which can gen single-col data according to a single-col config file. In `gen_workloads.py`, we read and process workload-config, **create single-col config files one by one** (which will be placed in `{target_basefile_name}/configs`), and call Generator class functions to get the final data column by column.

You can use `get_data_status.py` to get the data status of your generated data. Just run `python3 get_data_status.py gen_data`, and the result file will be placed in `{target_basefile_name}/outputs/stats.csv`.

## Sanity Check
In folder `sanity_check`, we can do per-column-level check and workload-level check. But you are not supposed to run these codes if you don't have the complete real-world data set.

## Others
If you want to sweep parameters, you can change the prob-vector in workload_config and make some value have the probability of 1.

 However, we don't randomly pick the features this time, so if you set the column num of 1, you will always get the quite similar features for all the times you run the generator, while these features used to change a lot randomly for every re-run.


