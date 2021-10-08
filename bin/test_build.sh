#!/bin/bash
set -exuo pipefail

# Prepare example directories.
git ls-files examples | (
    while read fn; do
        mkdir -p $(dirname "build/$fn");
	cp "$fn" "build/$fn";
    done)

# Initialize logs.
mkdir -p build/combined
echo -n > build/combined/redun.log

# Run individual examples.
(
    cd build/examples/simple
    ../../venv/bin/redun -c . run simple.py workflow
    ../../venv/bin/redun -c . export >> ../../combined/redun.log
)
(
    cd build/examples/bioinfo
    ../../venv/bin/redun -c . run pipeline.py workflow
    ../../venv/bin/redun -c . export >> ../../combined/redun.log
)
(
    cd build/examples/compile
    ../../venv/bin/redun -c . run make.py make
    ../../venv/bin/redun -c . export >> ../../combined/redun.log
)
(
    cd build/examples/etl
    ../../venv/bin/redun -c . run etl.py workflow
    ../../venv/bin/redun -c . export >> ../../combined/redun.log
)
(
    cd build/examples/recursion
    ../../venv/bin/redun -c . run fib.py fib --n 10
    ../../venv/bin/redun -c . export >> ../../combined/redun.log
)
(
    cd build/examples/higher_order
    ../../venv/bin/redun -c . run pipeline.py workflow
    ../../venv/bin/redun -c . export >> ../../combined/redun.log
)

# Combine callgraphs.
rm -rf build/combined/.redun
build/venv/bin/redun init build/combined/
build/venv/bin/redun -c build/combined/.redun import < build/combined/redun.log
