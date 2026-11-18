#!/bin/sh
set -eu
cd -- "$(dirname -- "$0")"
if [ "$#" -eq 0 ]; then
    set -- 2 data/ReviewsSample.csv output-step1 output
fi

hadoop jar target/Lab4-1.0.0.jar it.polito.bigdata.hadoop.lab.DriverBigData -Dmapreduce.framework.name=local -Dfs.defaultFS=file:/// "$@"
