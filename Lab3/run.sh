#!/bin/sh
set -eu
cd -- "$(dirname -- "$0")"
if [ "$#" -eq 0 ]; then
    set -- 2 data/AmazonTransposedDataset_Sample.txt output-step1 output
fi

hadoop jar target/Lab3-1.0.0.jar it.polito.bigdata.hadoop.lab.DriverBigData "$@"
