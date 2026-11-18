#!/bin/sh
set -eu
cd -- "$(dirname -- "$0")"
if [ "$#" -eq 0 ]; then
    set -- data/word-frequencies.tsv output ho
fi

hadoop jar target/Lab2-1.0.0.jar it.polito.bigdata.hadoop.lab.DriverBigData -Dmapreduce.framework.name=local -Dfs.defaultFS=file:/// "$@"
