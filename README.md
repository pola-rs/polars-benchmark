polars-tpch
===========

This repo contains the code used for performance evaluation of polars.

## Generating TPC-H Data

### Project setup

```shell
# clone this repository
git clone https://github.com/pola-rs/tpch.git
cd tpch/tpch-dbgen

# build tpch-dbgen
make
```

Notes:

- For MacOS, the above `make` command will result in an error while compiling like below,

   ```shell
   bm_utils.c:71:10: fatal error: 'malloc.h' file not found
   #include <malloc.h>
            ^~~~~~~~~~
   1 error generated.
   make: *** [bm_utils.o] Error 1
   ```
  To fix this, change the import statement `#include <malloc.h>` to `#include <sys/malloc.h>` in the files where error
  is reported (`bm_utils.c` and `varsub.c`) and then re-run the command `make`.

### Execute

```shell
# change directory to the root of the repository
cd ../
make run
```

This will do the following,

- Create a new virtual environment with all required dependencies.
- Generate data for benchmarks.
- Run the benchmark suite.
