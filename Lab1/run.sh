#!/bin/sh
set -eu
cd -- "$(dirname -- "$0")"
if [ "$#" -eq 0 ]; then
    set -- 2 example_data output
fi

hadoop jar target/Lab1-1.0.0.jar it.polito.bigdata.hadoop.DriverBigData "$@"
