# PBC

This is the Code for reproducing the experimental results in the SIGMOD 2024 submission:  High-Ratio Compression for Machine-Generated Data.

Note that this implement is only for testing and verifying the idea of the proposed pattern-based compression.

An industrial implement that will be applied to our DB system, a Redis-compatible, durable, in-memory database service (product name and company name omitted due to the submission rule) is Work-In-Process and will be open-source later.


# Dependences

Please make the following libraries are installed.

```
gcc >= 4.8.1
cmake >= 2.8.11  
python >= 2.7    
boost >= 1.57
```



## Build

```
./build.sh -r
```

## Pattern Extraction

Please run the bash file to apply pattern extraction.

```
./bin/pbc-cli --train-pattern --file-path <datasets file path> --pattern-path <patterns file path> --pattern-size <target pattern size(byte)> --train-value-number <the number of the training data>
```

For example:

```
./bin/pbc-cli --train-pattern --file-path data/datasets/cities --pattern-path data/patterns/cities.pattern --pattern-size 2000 --train-value-number 5000
```

## Compression and Decompression Test

Please use the following command to run compression and decompression test of the plain $PBC$.

```
./bin/pbc-cli --test-compress --file-path <datasets file path> --pattern-path <patterns file path>
```

For example:

```
./bin/pbc-cli --test-compress --file-path data/datasets/cities --pattern-path data/patterns/cities.pattern
```

we also provide an option to further compress data by [FSE](https://github.com/Cyan4973/FiniteStateEntropy), just using --test-compress-fse instead of --test-compress

### Output

It will print the following metrics:

Matching ratio: The ratio of outliers.

Compression ratio: Compressed Size/Raw Size

Compression Speed and Decompression Speed in (MB/s)

#### 

#### Example:

Compressing cities:
test compress
pattern path:data/datasets/cities.pattern
compression rate: 0.196698
compression speed: 56.9287MB/s
decompression speed: 1138.06MB/s

### Various Encoders

To reproducing the results of $PBC_{F}$, $PBC_{Z}$ and $PBC_{L}$, an intermediate representation of strings will be generated (by adding --compressed-file-path \<output file path\>) after running the Compress and Decompress program. 

Then we can use [Zstd](https://github.com/facebook/zstd) or [FSST](https://github.com/cwida/fsst) to compress the intermediate representation to reproducing the results of $PBC_{F}$.


In detail, to reproducing the $PBC_{F}$

Please install [FSST](https://github.com/cwida/fsst), and then compress the intermediate representation of strings using ./additional_options/line_fsst.cpp. 

Compile it first (this cpp file should be putted in ./fsst-master/paper) with:

```
g++ -std=c++14 -W -Wall  -O3 -g -ohcw -DNONOPT_FSST -I.. ../libfsst.cpp line_fsst.cpp -o line_fsst
```

And run with:

```
./line_fsst fsst <the intermediate file of strings>
```



To reproducing the $PBC_{Z}$, $PBC_{L}$, please install [LZBENCH](https://github.com/inikep/lzbench) fisrt. 
Then, compress the intermediate string representation using

```
 ./lzbench -ezstd,3/lzma,6/lz4/snappy  <the intermediate file of strings>
```
Note that an end to end implement will be given in the open-source industrial version. 





### Baselines
[FSST, LZ4dict, ZstdDict](https://github.com/cwida/fsst) 

[Zstd, LZMA, LZ4, Snappy](https://github.com/inikep/lzbench)





### Input Data Format

PBC is line-wise compression. 

Thus the data should be that each line store a string. 

Example:

```shell
string1
string2
string3
...
```







We have provided the patterns we used in the experiment for reproducing. 

The patterns we used is available in ./data/patterns. 

According to the company privacy policy, the 5 key-value dataset can not be provided yet (we wil try to open-source them or provide some samples after removing sensetive words). 
All the other datasets we used can be downloaded in the following links:
https://doi.org/10.5281/zenodo.1144100
https://github.com/THUBear-wjy/openSample
https://github.com/cwida/fsst
All the source of these datasets are cited in our paper. 

