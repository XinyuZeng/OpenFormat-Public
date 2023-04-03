# OpenFormat
The `benchmark` directory is for real-world data set distribution analysis and benchmark generator, `python` directory is for experiment running and ploting scripts.

Executables to test the scan performance of Parquet and ORC refer to separate repositories arrow-Public, arrow-rs and orc.

To run the experiments, you need to make sure all the repos are in the same home directory. For example:

```
/home/user/
    \____ OpenFormat 
           \____ benchmark (real-world data analysis and benchmark generator)
           \____ python (experiment automation scripts and plotting)
    \____ arrow-Public (including executables and profiling utilities for testing Parquet's scan performance, and also nested overhead for Parquet and ORC to Arrow)
    \____ orc (including executables and profiling utilities for testing ORC's scan and select performance)
    \____ arrow-rs (including executables for testing Parquet's select performance)
```

You need to follow each repo's build instructions to install the dependencies of arrow, orc, and arrow-rs, and then build each repo in release mode.

arrow-Public is built using cmake preset "openformat-release".

Some utilities from arrow-rs need to be installed globally (i.e., add to PATH) to run the experiments. You can go to arrow-rs directory and run `cargo install --path parquet --features=cli` to install them.

## Benchmark
You should firstly get into `benchmark/generator_v2` and read `README.md`

### Multi Workloads
We have provided four typical workloads:
* `classic`: the classic workloads of database or normal life, including movie records (`imdb`), business reviews (`yelp`), product prices (`UKPP`), recipe collection (`menu`).
* `geo`: the datasets about some geography or location information, including cell towers' location (`cells`), Geoname database (`geo`), flight information (`flight`).
* `log`: the datasets about the log of servers or websites, including machine log (`mgbench`), website click log (`edgar`).
*  `ml`: the machine learning dataset (`ml`).
*  `bi`: the Public BI benchmark (`cwi`).

The configs of each workload is in directory `benchmark/generator_v2/workload_config/{workload}_config/`. Predicate configs are stored separately in `benchmark/generator_v2/filter_config`.


## Experiment scripts

Experiment scripts are in `python` directory, with the experiments of each feature of format in a separate subdirectory.